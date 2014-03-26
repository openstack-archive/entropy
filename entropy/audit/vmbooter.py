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
import re
import time

from kombu import BrokerConnection
from kombu.common import maybe_declare
from kombu.pools import producers
from novaclient.client import Client
import paramiko

import base
from entropy.queues import entropy_exchange

LOG = logging.getLogger(__name__)


class Audit(base.AuditBase):
    # TODO(praneshp): this can be done with plumbum instead.
    @staticmethod
    def remote_call(cmd, **kwargs):
        jumphost = kwargs['jump_host']
        jump_user = kwargs['jump_user']
        LOG.info('running %s remotely on %s', cmd, jumphost)
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        ssh = paramiko.SSHClient()
        # NOTE(praneshp):  I'm working with a host I trust, this might
        # not be true always, so be careful
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=jumphost, username=jump_user)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        return {'exit_status': stdout.channel.recv_exit_status(),
                'stdout': stdout.readlines(),
                'stderr': stderr.readlines()}

    @staticmethod
    def flavor_list_with_novaclient(**kwargs):
        auth_url = 'http://{0}:5000/v2.0'.format(kwargs['api_host'])
        LOG.info('auth url is %s', auth_url)
        nc = Client(kwargs['nova_version'], kwargs['nova_username'],
                    kwargs['nova_password'], kwargs['nova_tenant'],
                    auth_url)
        flavors = nc.flavors.list()
        LOG.info('List of flavors: %s', flavors)

    @staticmethod
    def delete_with_cli(**kwargs):
        auth_url = 'http://{0}:{1}/{2}'.format(kwargs['api_host'],
                                               kwargs['auth_port'],
                                               kwargs['auth_version'])
        nova_prefix = 'nova --os-username {0} --os-password {1}  ' \
                      '--os-tenant-name {2}  ' \
                      '--os-auth-url {3} '.format(kwargs['nova_username'],
                                                  kwargs['nova_password'],
                                                  kwargs['nova_tenant'],
                                                  auth_url)
        list_command = nova_prefix + ' list'
        vm_list = Audit.remote_call(list_command, **kwargs)
        if vm_list['exit_status'] == 0:
            pattern = re.escape(kwargs['vm_id']) + r'-[0-9]*'
            vms = re.findall(pattern, ' '.join(vm_list['stdout']))
            uniq_vms = list(set(vms))
            LOG.info('Deleting %s', uniq_vms)
            delete_command = nova_prefix + ' delete ' + ' '.join(uniq_vms)
            return Audit.remote_call(delete_command, **kwargs)

        else:
            return vm_list

    @staticmethod
    def flavor_list_with_cli(**kwargs):
        auth_url = 'http://{0}:{1}/{2}'.format(kwargs['api_host'],
                                               kwargs['auth_port'],
                                               kwargs['auth_version'])
        flavor_command = 'nova --os-username {0} --os-password {1}  ' \
                         '--os-tenant-name {2}  ' \
                         '--os-auth-url {3} ' \
                         'flavor-list'.format(kwargs['nova_username'],
                                              kwargs['nova_password'],
                                              kwargs['nova_tenant'],
                                              auth_url)
        return Audit.remote_call(flavor_command,
                                 jump_host=kwargs['jump_host'],
                                 jump_user=kwargs['jum[_user'])

    @staticmethod
    def boot_vm_with_cli(**kwargs):
        delete_vms = Audit.delete_with_cli(**kwargs)
        # NOTE(praneshp): we dont care if delete passed or not. Just
        # boot vms, include both retuns in output
        auth_url = 'http://{0}:{1}/{2}'.format(kwargs['api_host'],
                                               kwargs['auth_port'],
                                               kwargs['auth_version'])
        boot_command = 'nova --os-username {0} --os-password {1}  ' \
                       '--os-tenant-name {2}  ' \
                       '--os-auth-url {3} ' \
                       'boot --flavor {4} --image {5} ' \
                       '{6}-{7}'.format(kwargs['nova_username'],
                                        kwargs['nova_password'],
                                        kwargs['nova_tenant'],
                                        auth_url,
                                        kwargs['flavor'],
                                        kwargs['image'],
                                        kwargs['vm_id'],
                                        int(time.time()))
        return {'delete_vms': delete_vms,
                'boot': Audit.remote_call(boot_command, **kwargs)}

    def send_message(self, **kwargs):
        connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                      '%(mq_host)s:%(mq_port)s//'
                                      % kwargs['mq_args'])
        message = {'From': __name__,
                   'Date': str(datetime.datetime.now())}
        with producers[connection].acquire(block=True) as producer:
            maybe_declare(entropy_exchange, producer.channel)
            message['payload'] = self.boot_vm_with_cli(**kwargs)
            producer.publish(message,
                             exchange=entropy_exchange,
                             routing_key='vmboot',
                             serializer='json')
