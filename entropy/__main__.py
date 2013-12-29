# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import argparse
import datetime
import logging
import os
import sys
import threading
import time

import croniter
import pause

sys.path.insert(0, os.path.join(os.path.abspath(os.pardir)))
sys.path.insert(0, os.path.abspath(os.getcwd()))

from entropy import audit
from entropy import utils

GOOD_MOOD = 1
SCRIPT_REPO = os.path.dirname(__file__)
LOG_REPO = os.path.join(os.path.dirname(__file__), 'logs')
LOG = logging.getLogger(__name__)


def validate_cfg(file):
    #TODO(praneshp): can do better here
    if GOOD_MOOD == 1:
        return True
    return False


def run_audit(**kwargs):
    # Put a message on the mq
    audit.send_message(**kwargs)


def start_audit(**kwargs):
    now = datetime.datetime.now()
    schedule = kwargs['schedule']
    cron = croniter.croniter(schedule, now)
    next_iteration = cron.get_next(datetime.datetime)
    while True:
        LOG.warning('Next call at %s' % next_iteration)
        pause.until(next_iteration)
        run_audit(**kwargs['mq_args'])
        next_iteration = cron.get_next(datetime.datetime)


def register_audit(args):
    LOG.warning('Registering audit script')

    #first check if you have all inputs
    if not (args.conf or args.script):
        LOG.warning('Need path to script and json')
        sys.exit(1)

    # Now validate cfg
    conf_file = os.path.join(SCRIPT_REPO, args.conf)
    validate_cfg(conf_file)

    # Now pick out relevant info
    # TODO(praneshp) eventually this must become a function call
    data = utils.load_yaml(conf_file)

    # stuff for the message queue
    mq_args = {'mq_host': data['mq_host'],
               'mq_port': data['mq_port'],
               'mq_user': data['mq_user'],
               'mq_password': data['mq_password']}

    # general stuff for the audit module
    kwargs = {'sshkey': utils.get_key_path(),
              'name': data['name'],
              'schedule': data['cron-freq'],
              'mq_args': mq_args}

    #Start a thread to run a cron job for this audit script
    t = threading.Thread(name=kwargs['name'], target=start_audit,
                         kwargs=kwargs)
    t.start()
    t.join()

    #TODO(praneshp): add this to a cfg file, to recover in case of failure


def register_repair(args):
    LOG.warning('Registering repair script')


def init():
    LOG.warning('Initializing')
    #TODO(praneshp): come up with  to start all registered reaction scripts


def parse():
    parser = argparse.ArgumentParser(description='entropy')
    subparsers = parser.add_subparsers(dest='command',
                                       help='commands')

    register_audit_parser = subparsers.add_parser('register-audit')
    register_audit_parser.add_argument('-f', dest='script',
                                       action='store', help='Audit script')
    register_audit_parser.add_argument('-c', dest='conf', action='store',
                                       help='Audit conf')
    register_audit_parser.set_defaults(func=register_audit)
    register_repair_parser =\
        subparsers.add_parser('register-repair',
                              help='Register a repair script')
    register_repair_parser.add_argument('-f', dest='filename', action='store',
                                        help='Repair script location')
    register_repair_parser.set_defaults(func=register_repair)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    #TODO(praneshp): AMQP, json->yaml, reaction scripts(after amqp)
    logging.basicConfig(filename=os.path.join(
                        LOG_REPO, 'entropy-' + str(time.time()) + '.log'))
    init()
    parse()
