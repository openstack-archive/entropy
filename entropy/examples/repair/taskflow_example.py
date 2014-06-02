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
import json
import logging
import os
import time

from kombu import BrokerConnection
from kombu.mixins import ConsumerMixin
import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow import task

LOG = logging.getLogger(__name__)


class SomeConsumer(ConsumerMixin):
    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.mq = kwargs['message_queue']
        self.name = kwargs['name']
        return

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.mq], callbacks=[self.on_message])]

    def on_message(self, body, message):
        LOG.warning("React script %s received message: %r", self.name, body)
        # here is an opportunity to not ack messages, do some work instead
        message.ack()
        return


def receive_message(**kwargs):
    connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                  '%(mq_host)s:%(mq_port)s//' % kwargs)
    with connection as conn:
        try:
            SomeConsumer(conn, **kwargs).run()
        except KeyboardInterrupt:
            LOG.warning('Quitting %s' % __name__)


def parse_conf(**kwargs):
    with open(kwargs['conf'], 'r') as json_data:
        data = json.load(json_data)
        # stuff for the message queue
        mq_args = {'mq_host': data['mq_host'],
                   'mq_port': data['mq_port'],
                   'mq_user': data['mq_user'],
                   'mq_password': data['mq_password']}
        parsed_args = data
        parsed_args['mq_args'] = mq_args
        for key in kwargs.keys():
            if key not in parsed_args:
                parsed_args[key] = kwargs[key]
        return parsed_args


def set_logger(logger, **kwargs):
    logger.handlers = []
    log_to_screen = logging.StreamHandler()
    log_to_screen.setLevel(logging.INFO)
    log_format = "%(lineno)s: %(message)s"
    formatter = logging.Formatter(log_format)
    log_to_screen.setFormatter(formatter)
    logger.addHandler(log_to_screen)


def main1(**kwargs):
    set_logger(LOG, **kwargs)
    LOG.info('starting react script %s', kwargs['name'])
    parsed_args = parse_conf(**kwargs)
    receive_message(**parsed_args)

class Foo(task.Task):
    def execute(self, x):
        LOG.error('%s in foo starting %s second sleep', 
                  datetime.datetime.now(), x)
        time.sleep(x)

class Bar(task.Task):
    def execute(self, y):
        LOG.error('%s in bar starting %s second sleep', 
                  datetime.datetime.now(), y)
        time.sleep(y)
        LOG.error('in bar after sleep %s', datetime.datetime.now())

# Create your flow and associated tasks (the work to be done).
flow = lf.Flow('simple-linear').add(
    Foo(),
    Bar()
)

def main():
    set_logger(LOG)
#    logging.basicConfig(
#        format='%(relativeCreated)5d %(name)-15s %(levelname)-8s %(message)s',
#        level=logging.DEBUG)
    LOG.error('taskflow example')
    taskflow.engines.run(flow, store=dict(x=1,y=2))


if __name__ == '__main__':
    main()
