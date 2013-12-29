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
import subprocess
import sys
import threading

import croniter
import pause
import yaml

sys.path.insert(0, os.path.join(os.path.abspath(os.pardir)))
sys.path.insert(0, os.path.abspath(os.getcwd()))

from entropy import audit
from entropy import globals
from entropy import utils

LOG = logging.getLogger(__name__)


def run_scheduler(args):
    LOG.info('Starting Scheduler')

    #Start react scripts
    with open(globals.REPAIR_CFG) as cfg:
        scripts = yaml.load_all(cfg)
        for script in scripts:
            print script
            setup_react(script)

    #Start audit scripts
    threads = []
    with open(globals.AUDIT_CFG, 'r') as cfg:
        scripts = yaml.load_all(cfg)
        for script in scripts:
            t = setup_audit(script)
            threads.append(t)

    # Now join on the threads so you run forever
    [t.join() for t in threads]


def validate_cfg(cfg_file):
    #TODO(praneshp): can do better here
    if globals.GOOD_MOOD == 1:
        return True
    return False


def add_to_list(type, **kwargs):
    cfg_file = globals.AUDIT_CFG if type == 'audit' else globals.REPAIR_CFG
    with open(cfg_file, "a") as cfg:
        cfg.write(yaml.dump(kwargs, canonical=False,
                            default_flow_style=False,
                            explicit_start=True))


def setup_audit(script):
    LOG.warning('Setting up %s' % script['name'])

    # Now pick out relevant info
    data = utils.load_yaml(script['conf'])

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
    return t


def setup_react(script):
    LOG.warning('Setting up %s' % script['name'])

    data = utils.load_yaml(script['conf'])
    react_script = data['script']

    cmd = ('python ' + os.path.join(globals.SCRIPT_REPO, react_script)).split()
    subprocess.Popen(cmd, stdin=None, stdout=None,
                     stderr=None, close_fds=True)


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


def check_duplicate(name, cfg_file):
    with open(cfg_file, 'r') as cfg:
        scripts = yaml.load_all(cfg)
        names = [script['name'] for script in scripts]
        if name in names:
            return True
    return False


def repair_present(name):
    return check_duplicate(name, globals.REPAIR_CFG)


def audit_present(name):
    return check_duplicate(name, globals.AUDIT_CFG)


def register_audit(args):
    LOG.warning('Registering audit script %s' % args.name)

    #First check if you have all inputs
    if not (args.conf or args.script or args.name):
        LOG.error('Need path to script and json')
        return

    # Now validate cfg
    validate_cfg(args.conf)

    #Check if this one is already present
    if audit_present(args.name):
        LOG.error('Audit already exists, not registering')
        return

    #Write to audit file
    audit_cfg_args = {'name': args.name,
                      'conf': args.conf}
    add_to_list('audit', **audit_cfg_args)
    LOG.info('Registered audit %s' % args.name)


def register_repair(args):
    LOG.warning('Registering repair script %s' % args.name)

     #First check if you have all inputs
    if not (args.conf or args.name):
        LOG.error('Need path to script and json')
        return

    # Now validate cfg
    validate_cfg(args.conf)

    #Check if this one is already present
    if repair_present(args.name):
        LOG.error('Repair script already exists, not registering')
        return

    #Write to audit file
    repair_cfg_args = {'name': args.name,
                       'conf': args.conf}
    add_to_list('repair', **repair_cfg_args)
    LOG.info('Registered repair script %s' % args.name)


def parse():
    parser = argparse.ArgumentParser(description='entropy')
    subparsers = parser.add_subparsers(dest='command',
                                       help='commands')

    register_audit_parser =\
        subparsers.add_parser('register-audit',
                              help='Register a repair script')
    register_audit_parser.add_argument('-n', dest='name',
                                       action='store', help='Name of auditor')
    register_audit_parser.add_argument('-f', dest='script',
                                       action='store', help='Audit script')
    register_audit_parser.add_argument('-c', dest='conf', action='store',
                                       help='Audit conf')
    register_audit_parser.set_defaults(func=register_audit)

    register_repair_parser =\
        subparsers.add_parser('register-repair',
                              help='Register a repair script')
    register_repair_parser.add_argument('-n', dest='name', action='store',
                                        help='Repair script name')
    register_repair_parser.add_argument('-c', dest='conf', action='store',
                                        help='Repair conf')
    register_repair_parser.set_defaults(func=register_repair)

    scheduler_parser = subparsers.add_parser('start-scheduler',
                                             help='Start scheduler')
    scheduler_parser.add_argument('-v', dest='verbose', help='Verbosity')
    scheduler_parser.set_defaults(func=run_scheduler)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    #TODO(praneshp): AMQP, json->yaml, reaction scripts(after amqp)

    globals.init()
    logging.basicConfig(filename=globals.log_file,
                        level=logging.INFO)
    parse()
