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

from kombu import BrokerConnection
from kombu.common import maybe_declare
from kombu.pools import producers

from entropy.audit import base

LOG = logging.getLogger(__name__)


class Audit(base.AuditBase):
    def send_message(self, **kwargs):
        Audit.set_logger(LOG, **kwargs)
        connection = BrokerConnection('amqp://%(mq_user)s:%(mq_password)s@'
                                      '%(mq_host)s:%(mq_port)s//' %
                                      kwargs['mq_args'])
        message = {'From': kwargs['name'],
                   'Date': str(datetime.datetime.now().isoformat())}
        with producers[connection].acquire(block=True) as producer:
            try:
                maybe_declare(kwargs['exchange'], producer.channel)
                producer.publish(message,
                                 exchange=self.exchange,
                                 routing_key=self.routing_key,
                                 serializer='json')
                LOG.debug("%s published message", self.name)
            except Exception:
                LOG.exception("%s could not send message", self.name)
