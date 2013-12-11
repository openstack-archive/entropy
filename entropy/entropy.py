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
import json
import os
from os.path import join
import sys
import threading
import time

good_mood = 1
script_repo = os.getcwd()
ssh_repo = join('/Users/praneshp/', '.ssh')


def validate_cfg(file):
    if good_mood == 1:
        return True
    return False


def do_something():
    pass


def start_audit(**kwargs):
    time.sleep(5)
    with open(join(os.getcwd(), 'test'), "a") as op:
        op.write('starting audit ' + str(time.time()))

    #TODO(praneshp): Start croniter job here
    now = datetime.datetime.now()
    schedule = kwargs['schedule']
    cron = croniter.croniter(schedule, now)
    next_iter = cron.get_next(datetime)
    while True:
        now = datetime.datetime.now()
        if now > next_iter:
            do_something()
            next_iter = cron.get_next(schedule, datetime)
            time.sleep()
        else:
            time.sleep(60 * 60 * 2)  # sleep 2 hours


def register_audit(args):
    print 'Registering audit script'

    #first check if you have all inputs
    if not (args.conf or args.script):
        print "Need path to script and json"
        sys.exit(1)

    # Now validate cfg
    conf_file = join(script_repo, args.conf)
    validate_cfg(conf_file)
    print conf_file
    # Now pick out relevant info
    kwargs = {}
    with open(conf_file, 'r') as json_data:
        data = json.load(json_data)
        kwargs['username'] = data['username']
        kwargs['sshkey'] = join(ssh_repo, data['ssh-key'])
        kwargs['name'] = data['name']
        kwargs['schedule'] = data['cron-freq']

    #Start a thread to run a cron job for this audit script
    t = threading.Thread(name=kwargs['name'], target=start_audit,
                         kwargs=kwargs)
    t.start()
    t.join()

    #TODO(praneshp): add this script to the db, to recover in case of failures


def register_repair(args):
    print 'Registering repair script'


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
    parse()
