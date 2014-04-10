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
import json
import logging
import os

from kombu import BrokerConnection
from kombu.mixins import ConsumerMixin

from entropy.queues import pass_events


LOG = logging.getLogger(__name__)
LOG_REPO = os.path.join(os.getcwd(), 'entropy', 'logs')


def get_vm_count(body, **kwargs):
    LOG.info("Received message: %r" % body)
    try:
        payload = body['payload']
        for host, count in payload['vm_count'].items():
            if count > kwargs['limit']:
                LOG.info("Host %s has %s vms, more than %s",
                         host, count, kwargs['limit'])
            elif count == -1:
                LOG.error("Libvirt errored out when connecting to %s", host)

    except Exception as e:
        LOG.error(e)


class SomeConsumer(ConsumerMixin):
    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.args = kwargs
        return

    def get_consumers(self, consumer, channel):
        return [consumer(pass_events, callbacks=[self.on_message])]

    def on_message(self, body, message):
        get_vm_count(body, **self.args)
        message.ack()
        return


def recv_message(**kwargs):
    connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                  '%(mq_host)s:%(mq_port)s//'
                                  % kwargs['mq_args'])
    with connection as conn:
        try:
            SomeConsumer(conn, **kwargs).run()
        except KeyboardInterrupt:
            LOG.warning('Quitting %s' % __name__)


def parse_conf(conf):
    with open(conf, 'r') as json_data:
        data = json.load(json_data)
        # stuff for the message queue
        mq_args = {'mq_host': data['mq_host'],
                   'mq_port': data['mq_port'],
                   'mq_user': data['mq_user'],
                   'mq_password': data['mq_password']}
        kwargs = data
        kwargs['mq_args'] = mq_args
        return kwargs


def main(**kwargs):
    LOG.info('starting react script %s' % kwargs['name'])
    args = parse_conf(kwargs['conf'])
    recv_message(**args)


if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_REPO, 'react.log'))
    main()
