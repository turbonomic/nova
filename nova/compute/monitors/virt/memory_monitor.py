# Copyright 2014 VMTurbo Inc.
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
Memory monitor based on compute driver to retrieve Memory information
"""

from oslo.config import cfg

from nova.compute import monitors
from nova.compute.monitors import memory_monitor as monitor
from nova import exception
from ceilometer.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.openstack.common import timeutils

CONF = cfg.CONF
CONF.import_opt('compute_driver', 'nova.virt.driver')
LOG = logging.getLogger(__name__)


class ComputeDriverMemoryMonitor(monitor._MemoryMonitorBase):
    """Memory monitor based on compute driver

    The class inherits from the base class for resource monitors,
    and implements the essential methods to get metric names and their real
    values for Memory utilization.

    The compute manager could load the monitors to retrieve the metrics
    of the devices on compute nodes and know their resource information
    periodically.
    """

    def __init__(self, parent):
        super(ComputeDriverMemoryMonitor, self).__init__(parent)
        self.source = CONF.compute_driver
        self.driver = self.compute_manager.driver
        self._memory_stats = {}

    @monitors.ResourceMonitorBase.add_timestamp
    def _get_memory_used(self, **kwargs):
        return self._data.get("compute.node.memory.used")

    def _update_data(self, **kwargs):
        # Don't allow to call this function so frequently (<= 1 sec)
        now = timeutils.utcnow()
        if self._data.get("timestamp") is not None:
            delta = now - self._data.get("timestamp")
            if delta.seconds <= 1:
                return

        self._data = {}
        self._data["timestamp"] = now

        # Extract node's Memory statistics.
        try:
            memory_used = self.driver.get_memory_mb_used()
            self._data["compute.node.memory.used"] = memory_used
        except (NotImplementedError, TypeError, KeyError) as ex:
            LOG.exception(_("Not all properties needed are implemented "
                "in the compute driver: %s"), ex)
            raise exception.ResourceMonitorError(
                monitor=self.__class__.__name__)

        self._memory_stats = memory_used
