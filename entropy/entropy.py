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
import croniter
import datetime
import utils
import json
import logging
import os
from os import path
import sys
import threading
import time
good_mood = 1
script_repo = os.getcwd()
LOG_REPO = os.path.join(os.getcwd(), 'logs')


def validate_cfg(file):
    if good_mood == 1:
        return True
    return False


def do_something():
    with open(path.join(os.getcwd(), 'test'), "a") as op:
        op.write('starting audit ' + str(datetime.datetime.now()) + '\n')


def start_audit(**kwargs):
    #TODO(praneshp): Start croniter job here
    now = datetime.datetime.now()
    schedule = kwargs['schedule']
    cron = croniter.croniter(schedule, now)
    next_iteration = cron.get_next(datetime.datetime)
    while True:
        now = datetime.datetime.now()
        logging.warning(str(now) + str(next_iteration))
        if now > next_iteration:
            do_something()
            next_iteration = cron.get_next(datetime.datetime)
        else:
            sleep_time = (next_iteration - now).total_seconds()
            logging.warning('Will sleep for ' + str(sleep_time))
            time.sleep(sleep_time)


def register_audit(args):
    logging.warning('Registering audit script')

    #first check if you have all inputs
    if not (args.conf or args.script):
        logging.warning('Need path to script and json')
        sys.exit(1)

    # Now validate cfg
    conf_file = path.join(script_repo, args.conf)
    validate_cfg(conf_file)
    # Now pick out relevant info
    kwargs = {}
    with open(conf_file, 'r') as json_data:
        data = json.load(json_data)
        kwargs['username'] = data['username']
        # TODO(praneshp) eventually this must become a function call
        # somewhere else
        kwargs['sshkey'] = utils.get_key_path()
        kwargs['name'] = data['name']
        kwargs['schedule'] = data['cron-freq']

    #Start a thread to run a cron job for this audit script
    t = threading.Thread(name=kwargs['name'], target=start_audit,
                         kwargs=kwargs)
    t.start()
    t.join()

    #TODO(praneshp): add this to a cfg file, to recover in case of failure


def register_repair(args):
    logging.warning('Registering repair script')


def init():
    logging.warning('Initializing')


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
    logging.basicConfig(filename=os.path.join(
                        LOG_REPO, 'entropy-' + str(time.time()) + '.log'))
    parse()
