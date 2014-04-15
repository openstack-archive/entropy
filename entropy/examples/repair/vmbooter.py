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


def vmboot(body, **kwargs):
    try:
        payload = body['payload']
        LOG.info('Got %s', payload.keys())
        for key in payload.keys():
            if payload[key]['exit_status'] == 0:
                LOG.info('%s successful, stdout is %s',
                         key,
                         payload[key]['stdout'])
            else:
                LOG.error('%s failed with exit code %s',
                          key,
                          payload[key]['exit_status'])
                LOG.error('stdout: %s ', payload[key]['stdout'])
                LOG.error('stderr: %s ', payload[key]['stderr'])
    except Exception as e:
        LOG.exception(e)


class SomeConsumer(ConsumerMixin):
    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.args = kwargs
        return

    def get_consumers(self, consumer, channel):
        return [consumer(pass_events, callbacks=[self.on_message])]

    def on_message(self, body, message):
        vmboot(body, **self.args)
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


def set_logger(logger, **kwargs):
    logger.handlers = []
    log_to_file = logging.FileHandler(kwargs['log_file'])
    log_to_file.setLevel(logging.DEBUG)
    log_format = logging.Formatter(kwargs['log_format'])
    log_to_file.setFormatter(log_format)
    logger.addHandler(log_to_file)
    logger.propagate = False


def main(**kwargs):
    set_logger(LOG, **kwargs)
    LOG.info('starting react script %s' % kwargs['name'])
    args = parse_conf(kwargs['conf'])
    recv_message(**args)
