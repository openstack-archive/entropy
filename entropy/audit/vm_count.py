# Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import datetime
import logging

from fabric.api import env
from fabric.api import execute
from fabric.api import sudo
from kombu import BrokerConnection
from kombu.common import maybe_declare
from kombu.pools import producers

import base
from entropy.queues import entropy_exchange

LOG = logging.getLogger(__name__)


class Audit(base.AuditBase):
    def test(self):
        LOG.info('hello world')

    @staticmethod
    def get_hv_vm_count():
        return int(sudo("virsh list | grep -c running", quiet=True))

    def get_vm_count(self, **kwargs):
        vm_counts = {}
        env.hosts = kwargs['compute_hosts']
        results = execute(self.get_hv_vm_count)
        for (hostname, num_running) in results.items():
            LOG.debug("Host {0} has {1} running VMs".format(hostname,
                                                            num_running))
            vm_counts[hostname] = num_running
        return vm_counts

    def send_message(self, **kwargs):
        connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                      '%(mq_host)s:%(mq_port)s//'
                                      % kwargs['mq_args'])
        message = {'From': __name__,
                   'Date': str(datetime.datetime.now())}
        with producers[connection].acquire(block=True) as producer:
            maybe_declare(entropy_exchange, producer.channel)
            msg_args = {'vm_count': self.get_vm_count(**kwargs)}
            message['payload'] = msg_args
            producer.publish(message,
                             exchange=entropy_exchange,
                             routing_key='vmcount',
                             serializer='json')
