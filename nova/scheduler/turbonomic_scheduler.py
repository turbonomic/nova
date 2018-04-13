# Copyright (c) 2018 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Turbonomic Scheduler implementation
--------------------------------
Our scheduler works as a replacement for the default filter_scheduler

For integrating this scheduler to get Placement recommendations,
the following entries must be added in the /etc/nova/nova.conf file
under the [DEFAULT] section
------------------------------------------------------------
scheduler_driver = nova.scheduler.turbonomic_scheduler.TurbonomicScheduler
turbonomic_protocol = <Turbonomic_Protocol>
turbonomic_address = <Turbonomic_Address>
turbonomic_target_address = <Turbonomic_Target_Address>
turbonomic_timeout = <Turbonomic_Timeout>
turbonomic_username = <Turbonomic_UserName>
turbonomic_password = <Turbonomic_Password>
turbonomic_verify_ssl = <Verify_ssl_certificate, defaults to False>
------------------------------------------------------------
NOTE: 1) 'scheduler_driver' might already be configured to the default scheduler
       Needs to be replaced if that's the case

      2) scheduler_driver should be enabled across all regions, turbonomic_target_address must be equal to the address specified
      by the customer while discovering the target, e.x. a target consists of RegionOne (X.X.X.10) and RegionTwo (X.X.X.11)
      turbonomic_target_address must be set equal to X.X.X.10 both in RegionOne and RegionTwo

      3) In order to force NOVA deploy a new VM on a specific host, run the following command:
        nova boot --flavor <FLAVOR_ID> --image <IMG_UUID> --nic net-id=<NIC_ID> --availability-zone <AVAILABILITY_ZONE>:<HOST_NAME> <VM_NAME>

      4) In order to force NOVA deploy a new VM in an affinity group, run the following command:
        nova boot --flavor <FLAVOR_ID> --image <IMG_UUID> --nic net-id=<NIC_ID> --availability-zone <AZ,e.x. nova> --hint group=<AFFINITY_GROUP_UUID> <VM_NAME>

      5) This script should be placed to /lib/python2.7/site-packages/nova/scheduler

      6) This script is designed for OpenStack Mitaka

    At the time of writing features 3 and 4 were unavailable in OpenStack UI and could be used only from CLI.
"""

from oslo_config import cfg
from oslo_log import log as logging

import nova.conf
from nova import exception
from nova.i18n import _
from nova import rpc
from nova.scheduler import driver
from nova.scheduler import scheduler_options


import requests
from requests import exceptions
import json
import uuid

ext_opts = [
    cfg.StrOpt('turbonomic_protocol', default='https', help='turbonomic Server protocol, http or https'),
    cfg.StrOpt('turbonomic_address', default='URI', help='turbonomic Server address'),
    cfg.StrOpt('turbonomic_target_address', default='default-address', help='OSP target address'),
    cfg.StrOpt('turbonomic_timeout', default='60', help='turbonomic request timeout'),
    cfg.StrOpt('turbonomic_username', default='administrator', help='turbonomic Server Username'),
    cfg.StrOpt('turbonomic_password', default='administrator', help='turbonomic Server Password'),
    cfg.StrOpt('turbonomic_verify_ssl', default='False', help='Verify SSL certificate'),
]

CONF = nova.conf.CONF
CONF.register_opts(ext_opts)
LOG = logging.getLogger(__name__)

class TurbonomicScheduler(driver.Scheduler):
    def __init__(self, *args, **kwargs):
        super(TurbonomicScheduler, self).__init__(*args, **kwargs)
        self.turbonomic_rest_endpoint = CONF.turbonomic_protocol + "://" + CONF.turbonomic_address + "/vmturbo/rest/"
        self.turbonomic_target_address = CONF.turbonomic_target_address
        self.auth = (CONF.turbonomic_username, CONF.turbonomic_password)
        self.notifier = rpc.get_notifier('scheduler')
        self.j_session_id = None
        self.region = None
        self.turbonomic_timeout = int(CONF.turbonomic_timeout)
        self.verify_ssl = ('true' == CONF.turbonomic_verify_ssl.lower())
        LOG.info('Initialized, TurbonomicRestApiEndpoint: {}, TurbonomicTargetAddress: {}, verify_ssl: {}, timeout: {}'.format(
            self.turbonomic_rest_endpoint, self.turbonomic_target_address, self.verify_ssl, self.turbonomic_timeout))

    def select_destinations(self, context, spec_obj):
        self.notifier.info(context, 'turbonomic_scheduler.select_destinations.start',
                           dict(request_spec=spec_obj.to_legacy_request_spec_dict()))
        LOG.info('Selecting destinations, CTX: {}'.format(str(context)))

        self.schedule = False
        self.login()
        if self.j_session_id is None:
            raise exception.NoValidHost(reason='Error authenticating as {}'.format(self.auth[0]))

        selected_hosts = self.create_reservation(context, spec_obj)
        LOG.info('Selected hosts: {}'.format(str(selected_hosts)))
        if len(selected_hosts) == 0:
            raise exception.NoValidHost(reason='No suitable host found for flavor {}, num of VMs: {}'.format(self.flavor_name, self.vmCount))

        try:
            host_info = self.host_manager.get_all_host_states(context)
        except all:
            host_info = []

        destinations = []

        for host_item in host_info:
            hostname = host_item.host
            nodename = host_item.nodename
            for selected_host in selected_hosts:
                if hostname == selected_host:
                    vmt_host = {"host": hostname, "nodename": nodename, "limits": {}}
                    destinations.append(vmt_host)

        LOG.info('Destinations: {}'.format(str( destinations )))
        return destinations

    def login(self):
        LOG.info('Logging in to {}'.format(self.turbonomic_rest_endpoint + 'login'))
        try:
            auth_response = requests.post(self.turbonomic_rest_endpoint + "login", {'username': self.auth[0], 'password': self.auth[1]},
                                      verify = self.verify_ssl, timeout = self.turbonomic_timeout)

            if auth_response.status_code == 200:
                self.j_session_id = auth_response.cookies['JSESSIONID']
                LOG.info('Authenticated as {}'.format(self.auth[0]))
            else:
                LOG.info('Error authenticating as {}'.format(self.auth[0]))
                raise Exception('Authentication error')

        except exceptions.ReadTimeout:
            LOG.info('Login request timed out: {}, username: {} '.format(self.turbonomic_rest_endpoint + "login",
                                                                                    self.auth[0]))
            raise Exception('Login request timed out')

    def get_dc_uuid(self, availability_zone):
        LOG.info('Searching for DC: target: {}, AZ: {}'.format(self.turbonomic_target_address, availability_zone))
        try:
            entities_resp = requests.get(self.turbonomic_rest_endpoint + 'search?types=DataCenter',
                                     cookies={'JSESSIONID': self.j_session_id}, verify=self.verify_ssl, timeout = self.turbonomic_timeout)

            entities = entities_resp.json()
            for ent in entities:
                dc_uuid = ent.get('uuid', '')
                dc_uuid_parts = dc_uuid.split(':')
                if len(dc_uuid_parts) == 5 and 'OSS' == dc_uuid_parts[0] and 'DC' == dc_uuid_parts[3] and \
                                self.turbonomic_target_address == dc_uuid_parts[1] and availability_zone == dc_uuid_parts[4]:
                    self.region = dc_uuid_parts[2]
                    return dc_uuid

            raise exception.NoValidHost(reason='Region not found for target {}, AZ: {}'.format(self.turbonomic_target_address,
                                                                                               availability_zone))

        except exceptions.ReadTimeout:
            LOG.info('DC search request timed out: {}'.format(self.turbonomic_rest_endpoint + 'search?types=DataCenter'))
            raise exception.NoValidHost(reason='DC search request timed out')


    def get_template_uuid(self, template_name):
        full_template_name = '{}:{}::TMP-{}'.format(self.turbonomic_target_address, self.region, template_name)
        try:
            templates_response = requests.get(self.turbonomic_rest_endpoint + 'templates', cookies={'JSESSIONID': self.j_session_id},
                                          verify = self.verify_ssl, timeout = self.turbonomic_timeout)

            templates = json.loads(templates_response.content)
            for template in templates:
                temp_name = template.get('displayName', '')
                if full_template_name == temp_name:
                    return template.get('uuid', '')

            raise exception.NoValidHost(reason='No template found for {}'.format(full_template_name))

        except exceptions.ReadTimeout:
            LOG.info('Template request timed out: {}'.format(self.turbonomic_rest_endpoint + 'templates'))
            raise exception.NoValidHost(reason='Template request timed out')

    def create_reservation(self, context, spec_obj):
        self.reservationName = "OpenStack-Placement-Request-" + str(uuid.uuid4())
        self.flavor_name = spec_obj.flavor.name
        if 'id' in spec_obj.image:
            self.deploymentProfile = spec_obj.image.id
        else:
            self.deploymentProfile = ""
        self.vmCount = spec_obj.num_instances
        self.scheduler_hint = ''
        self.isSchedulerHintPresent = False
        if spec_obj.scheduler_hints is not None:
            if 'group' in spec_obj.scheduler_hints:
                self.scheduler_hint = spec_obj.scheduler_hints['group']
                if self.scheduler_hint is not None:
                    self.isSchedulerHintPresent = True
                else:
                    self.scheduler_hint = ''

        LOG.info('Flavor: {}, Img: {}'.format(str(spec_obj.flavor), str(spec_obj.image)))
        LOG.info('spec_obj: {}'.format(str(spec_obj)))
        selected_hosts = []

        if spec_obj.force_hosts is not None and len(spec_obj.force_hosts) > 0 and spec_obj.force_hosts[0] is not None:
            for force_host in spec_obj.force_hosts:
                selected_hosts.append(force_host)

            LOG.info('force_host = {}'.format(str(selected_hosts)))
            return selected_hosts

        if spec_obj.availability_zone is None:
            raise Exception('Availability zone not set')

        if context.remote_address is None:
            raise Exception('Remote address not set')

        dc_uuid = self.get_dc_uuid(spec_obj.availability_zone)

        template_uuid = self.get_template_uuid(self.flavor_name)

        LOG.info('Creating placement {}, DeploymentProfile: {}, SchedulerHint: {}, Template: {}, DC: {}' .format(
            self.reservationName, self.deploymentProfile, str(self.scheduler_hint), template_uuid, dc_uuid))

        if self.isSchedulerHintPresent:
            constraints = ''
            for i in range(0, len(self.scheduler_hint)):
                constraints += '"' + self.scheduler_hint[i] + '"'
                if i < len(self.scheduler_hint) - 1:
                    constraints += ','

            placement = '{"demandName": "' + self.reservationName + '", "action": "PLACEMENT", "parameters": [ ' \
                '{"placementParameters": {"count": ' + str(self.vmCount) + ', "templateID": "' + template_uuid + '", "constraintIDs":["' + dc_uuid + '"]},' \
                '"deploymentParameters": {"deploymentProfileID": "' + self.deploymentProfile + '", "constraintIDs":[' + constraints + ']  }}]}'
        else:
            placement = '{"demandName": "' + self.reservationName + '", "action": "PLACEMENT", "parameters": [ ' \
                    '{"placementParameters": {"count": ' + str(self.vmCount) + ', "templateID": "' + template_uuid + '", "constraintIDs":["' + dc_uuid + '"]},' \
                    '"deploymentParameters": {"deploymentProfileID": "' + self.deploymentProfile + '"}}]}'

        LOG.info('Placement json: {}'.format(placement))

        try:
            placement_response = requests.post(self.turbonomic_rest_endpoint + 'reservations', data=placement,
                                               cookies={'JSESSIONID': self.j_session_id}, headers={'content-type': 'application/json'},
                                               verify = self.verify_ssl, timeout = self.turbonomic_timeout)

            if placement_response.status_code == 200:
                placement = placement_response.json()
                LOG.info('Placement resp: {}'.format(str(placement_response)))
                LOG.info('Placement obj: {}'.format(str(placement)))
                if placement['status'] == 'PLACEMENT_SUCCEEDED':
                    if 'demandEntities' in placement:
                        demand_entities = placement['demandEntities']
                        for demand_entity in demand_entities:
                            if 'placements' in demand_entity:
                                placements = demand_entity['placements']
                                if 'computeResources' in placements:
                                    compute_resources = placements['computeResources']
                                    for cr in compute_resources:
                                        if 'provider' in cr:
                                            provider = cr['provider']
                                            if provider['className'] == 'PhysicalMachine':
                                                LOG.info('Appending host: {}'.format(provider['displayName']))
                                                selected_hosts.append(provider['displayName'])
                                else:
                                    LOG.info('No compute resource found in placements')
                            else:
                                LOG.info('No placement found')
                    else:
                        LOG.info('No demand entities found')
                else:
                    LOG.info('Placement failed: {}'.format(str(placement)))
            else:
                resp = placement_response.json()
                LOG.info('Error creating placement: {}'.format(str(resp)))
                raise Exception(resp['message'])

            return selected_hosts

        except exceptions.ReadTimeout:
            LOG.info('Placement request timed out {}'.format(self.turbonomic_rest_endpoint + 'reservations'))
            raise Exception('Placement request timed out')
