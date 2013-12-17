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

from queues import pass_events


SCRIPT_REPO = os.path.dirname(__file__)
conf_file = os.path.join(SCRIPT_REPO, 'react.json')
LOG = logging.getLogger(__name__)

class SomeConsumer(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, Consumer, channel):
        return [Consumer(pass_events, callbacks=[self.on_message])]

    def on_message(self, body, message):
        LOG.warning("Received message: %r" % body)
        message.ack()
        return


def recv_message(**kwargs):
    connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                  '%(mq_host)s:%(mq_port)s//' % kwargs)
    with connection as conn:
        try:
            SomeConsumer(conn).run()
        except KeyboardInterrupt:
            LOG.warning('Quitting %s' % __name__)


def parse_conf():
    with open(conf_file, 'r') as json_data:
        data = json.load(json_data)
        # stuff for the message queue
        mq_args = {'mq_host': data['mq_host'],
                   'mq_port': data['mq_port'],
                   'mq_user': data['mq_user'],
                   'mq_password': data['mq_password']}
        return mq_args


if __name__ == '__main__':
    #can log to stdout for now
    logging.basicConfig()
    LOG.warning('starting react script %s' % __file__)
    mq_args = parse_conf()
    recv_message(**mq_args)
