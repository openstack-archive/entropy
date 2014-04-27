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
import abc
import logging

from entropy import utils

LOG = logging.getLogger(__name__)


class AuditBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        utils.reset_logger(logging.getLogger())
        self.name = kwargs['name']
        self.exchange = kwargs['exchange']
        self.routing_key = kwargs['routing_key']

    @staticmethod
    def set_logger(logger, **kwargs):
        logger.handlers = []
        log_to_file = logging.FileHandler(kwargs['log_file'])
        log_to_file.setLevel(logging.DEBUG)
        log_format = logging.Formatter(kwargs['log_format'])
        log_to_file.setFormatter(log_format)
        logger.addHandler(log_to_file)
        logger.propagate = False

    @abc.abstractmethod
    def send_message(self, **kwargs):
        pass
