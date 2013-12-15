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
from kombu import BrokerConnection
from kombu.mixins import ConsumerMixin

from queues import pass_events


class SomeConsumer(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, Consumer, channel):
        return [Consumer(pass_events, callbacks=[self.on_message])]

    def on_message(self, body, message):
        print("Received message: %r" % body)
        message.ack()
        return


def recv_message():
    print "Started recv message"
    connection = BrokerConnection('amqp://guest:guest@localhost:5672//')
    with connection as conn:
        try:
            SomeConsumer(conn).run()
        except KeyboardInterrupt:
            print('Quitting')


if __name__ == '__main__':
    recv_message()
