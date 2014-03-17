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
    @staticmethod
    def boot_vm_with_novaclient(**kwargs):
        auth_url = 'http://{0}:5000/v2.0'.format(kwargs['api_host'])
        LOG.error('auth url is %s', auth_url)
        nc = Client(kwargs['nova_version'], kwargs['nova_username'],
                    kwargs['nova_password'], kwargs['nova_tenant'],
                    auth_url)
        flavors = nc.flavors.list()
        LOG.error('List of flavors: %s', flavors)

    @staticmethod
    def flavor_list_with_cli(**kwargs):
        auth_url = 'http://{0}:5000/v2.0'.format(kwargs['api_host'])
        flavor_command = 'nova --os-username {0} --os-password {1}  ' \
                         '--os-tenant-name {2}  ' \
                         '--os-auth-url {3} ' \
                         'flavor-list'.format(kwargs['nova_username'],
                                              kwargs['nova_password'],
                                              kwargs['nova_tenant'],
                                              auth_url)
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        LOG.error('command %s', flavor_command)
        ssh = paramiko.SSHClient()
        # NOTE(praneshp):  I'm working with a host I trust, this might
        # not be true always, so be careful
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jumphost = kwargs['jump_host']
        jump_user = kwargs['jump_user']
        ssh.connect(hostname=jumphost, username=jump_user)
        stdin, stdout, stderr = ssh.exec_command(flavor_command)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status == 0:
            LOG.error('Flavor-list is %s', stdout.readlines())
        else:
            LOG.error('Something wrong: ', stderr.readlines())

    @staticmethod
    def boot_vm_with_cli(**kwargs):
        auth_url = 'http://{0}:5000/v2.0'.format(kwargs['api_host'])
        flavor_command = 'nova --os-username {0} --os-password {1}  ' \
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
        LOG.error('command %s', flavor_command)
        # NOTE(praneshp) if we don't do this, paramiko will spit out
        # a lot of debug messages.
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        ssh = paramiko.SSHClient()
        # NOTE(praneshp):  I'm working with a host I trust, this might
        # not be true always, so be careful!!
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jumphost = kwargs['jump_host']
        jump_user = kwargs['jump_user']
        ssh.connect(hostname=jumphost, username=jump_user)
        stdin, stdout, stderr = ssh.exec_command(flavor_command)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status == 0:
            LOG.error('Flavor-list is %s', stdout.readlines())
        else:
            LOG.error('Something wrong: %s ', stderr.readlines())

    def send_message(self, **kwargs):
        connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                      '%(mq_host)s:%(mq_port)s//'
                                      % kwargs['mq_args'])
        message = {'From': __name__,
                   'Date': str(datetime.datetime.now())}
        with producers[connection].acquire(block=True) as producer:
            maybe_declare(entropy_exchange, producer.channel)
            msg_args = {'vm_count': self.boot_vm_with_cli(**kwargs)}
            message['payload'] = msg_args
            producer.publish(message,
                             exchange=entropy_exchange,
                             routing_key='vmcount',
                             serializer='json')
