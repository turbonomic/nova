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
turbonomic_rest_uri = <Turbonomic_IPAddress>
turbonomic_username = <Turbonomic_UserName>
turbonomic_password = <Turbonomic_Password>
turbonomic_verify_ssl = <Verify_ssl_certificate, defaults to False>
------------------------------------------------------------
NOTE: 1) 'scheduler_driver' might already be configured to the default scheduler
       Needs to be replaced if that's the case

      2) scheduler_driver should be enabled across all regions.

      3) In order to force NOVA deploy a new VM on a specific host, run the following command:
        nova boot --flavor <FLAVOR_ID> --image <IMG_UUID> --nic net-id=<NIC_ID> --availability-zone <AVAILABILITY_ZONE>:<HOST_NAME> <VM_NAME>

      4) In order to force NOVA deploy a new VM in an affinity group, run the following command:
        nova boot --flavor <FLAVOR_ID> --image <IMG_UUID> --nic net-id=<NIC_ID> --hint group=<AFFINITY_GROUP_UUID> <VM_NAME>

    At the time of writing features 3 and 4 were unavailable in OpenStack UI and could be used only from CLI

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
import json
import uuid

ext_opts = [
    cfg.StrOpt('turbonomic_rest_uri', default='URI', help='turbonomic Server URL'),
    cfg.StrOpt('turbonomic_username', default='VMT_USER', help='turbonomic Server Username'),
    cfg.StrOpt('turbonomic_password', default='VMT_PWD', help='turbonomic Server Username'),
    cfg.StrOpt('turbonomic_verify_ssl', default='False', help='Verify SSL certificate'),
]
CONF = nova.conf.CONF
CONF.register_opts(ext_opts)
LOG = logging.getLogger(__name__)

class TurbonomicScheduler(driver.Scheduler):
    def __init__(self, *args, **kwargs):
        super(TurbonomicScheduler, self).__init__(*args, **kwargs)
        self.vmt_url = 'http://' + CONF.turbonomic_rest_uri + "/vmturbo/rest/"
        self.auth = (CONF.turbonomic_username, CONF.turbonomic_password)
        self.notifier = rpc.get_notifier('scheduler')
        self.scheduler_ip = CONF.my_ip
        self.j_session_id = None
        self.verify_ssl = CONF.turbonomic_verify_ssl
        LOG.info('Initialized, verify_ssl: {}'.format(CONF.turbonomic_verify_ssl))

    def select_destinations(self, context, spec_obj):
        self.notifier.info(context, 'turbonomic_scheduler.select_destinations.start',
                           dict(request_spec=spec_obj.to_legacy_request_spec_dict()))
        LOG.info('Selecting destinations')

        self.schedule = False
        self.login()
        if self.j_session_id is None:
            raise exception.NoValidHost(reason='Error authenticating as {} / {}'.format(self.auth[0], self.auth[1]))

        selected_hosts = self.create_reservation(spec_obj)
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
            if hostname in selected_hosts:
                vmt_host = {"host": hostname, "nodename": nodename, "limits": {}}
                destinations.append(vmt_host)

        dests = [dict(host=host.get('host'), nodename=host.get('nodename'), limits=host.get('limits')) for host in destinations]
        LOG.info('Destinations: {}'.format(str( dests )))
        return dests

    def login(self):
        LOG.info('Logging to {}'.format(self.vmt_url + 'login'))
        auth_response = requests.post(self.vmt_url + "login", {'username': self.auth[0], 'password': self.auth[1]}, verify = self.verify_ssl)
        self.j_session_id = auth_response.cookies['JSESSIONID']
        if auth_response.status_code == 200:
            LOG.info('Authenticated as {}'.format(self.auth[0]))
        else:
            LOG.info('Error authenticating as {}'.format(self.auth[0]))
            raise Exception('Authentication error')

    def get_template_uuid(self, template_name):
        templates_response = requests.get(self.vmt_url + '/templates', cookies={'JSESSIONID': self.j_session_id}, verify = self.verify_ssl)
        templates = json.loads(templates_response.content)
        uuid = ''
        for template in templates:
            if template_name in template['displayName']:
                uuid = template['uuid']

        return uuid

    def create_reservation(self, spec_obj):
        self.reservationName = "Reservation-" + str(uuid.uuid4())
        self.vmPrefix = "VMTReservation"
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

        LOG.info('spec_obj: {}'.format(str(spec_obj)))
        selected_hosts = []

        if spec_obj.force_hosts is not None and len(spec_obj.force_hosts) > 0 and spec_obj.force_hosts[0] is not None:
            for force_host in spec_obj.force_hosts:
                selected_hosts.append(force_host)

            LOG.info('force_host = {}'.format(str(selected_hosts)))
            return selected_hosts

        template_uuid = self.get_template_uuid(self.flavor_name)

        LOG.info('Creating placement {}, Flavor: {}, DeploymentProfile: {}, SchedulerHint: {}, Template: {}' .format(
            self.reservationName, self.flavor_name, self.deploymentProfile, str(self.scheduler_hint), template_uuid))

        if self.isSchedulerHintPresent:
            constraints = ''
            for i in range(0, len(self.scheduler_hint)):
                constraints += '"' + self.scheduler_hint[i] + '"'
                if i < len(self.scheduler_hint) - 1:
                    constraints += ','

            placement = '{"demandName": "' + self.reservationName + '", "action": "PLACEMENT", "parameters": [ ' \
                '{"placementParameters": {"count": ' + str(self.vmCount) + ', "templateID": "' + template_uuid + '"},' \
                '"deploymentParameters": {"deploymentProfileID": "' + self.deploymentProfile + '", "constraintIDs":[' + constraints + ']  }}]}'
        else:
            placement = '{"demandName": "' + self.reservationName + '", "action": "PLACEMENT", "parameters": [ ' \
                    '{"placementParameters": {"count": ' + str(self.vmCount) + ', "templateID": "' + template_uuid + '"},' \
                    '"deploymentParameters": {"deploymentProfileID": "' + self.deploymentProfile + '"}}]}'

        LOG.info('Placement json: {}'.format(placement))

        placement_response = requests.post(self.vmt_url + '/reservations', data=placement, cookies={'JSESSIONID': self.j_session_id},
                                           headers={'content-type': 'application/json'}, verify = self.verify_ssl)

        if placement_response.status_code == 200:
            placement = placement_response.json()
            LOG.info('Placement created: {}'.format(str(placement)))
            if placement['status'] == 'PLACEMENT_SUCCEEDED':
                if 'demandEntities' in placement:
                    demandEntity = placement['demandEntities'][0]
                    if 'placements' in demandEntity:
                        placements = demandEntity['placements']
                        if 'computeResources' in placements:
                            computeResources = placements['computeResources']
                            for cr in computeResources:
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
                LOG.info('Placement failed: {}'.format(placement['status']))
        else:
            resp = placement_response.json()
            LOG.info('Error creating placement: {}'.format(str(resp)))
            raise Exception(resp['message'])

        return selected_hosts
