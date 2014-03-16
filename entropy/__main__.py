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
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.path.abspath(os.pardir)))
sys.path.insert(0, os.path.abspath(os.getcwd()))

from engine import Engine
from entropy import globals
from entropy import utils

LOG = logging.getLogger(__name__)
running_audits = []
running_repairs = []
executor = ThreadPoolExecutor(max_workers=globals.MAX_WORKERS)
all_futures = []
entropy_engine = None


# TODO(praneshp): for next 3 fns, read the right file from engine name and
# type, then modify that file.
def add_to_list(script_type, **kwargs):
    if script_type == 'audit':
        cfg_file = globals.AUDIT_CFG
    else:
        cfg_file = globals.REPAIR_CFG
    with open(cfg_file, "a") as cfg:
        cfg.write(yaml.dump(kwargs, canonical=False,
                            default_flow_style=False,
                            explicit_start=True))


def repair_present(name):
    return utils.check_duplicate(name, globals.REPAIR_CFG)


def audit_present(name):
    return utils.check_duplicate(name, globals.AUDIT_CFG)


def register_audit(args):
    #TODO(praneshp) check for sanity (file exists, imp parameters exist, etc)
    LOG.warning('Registering audit script %s', args.name)

    #First check if you have all inputs
    if not (args.conf and args.name and args.engine):
        LOG.error('Need path to script and json')
        return

    #Check if this one is already present
    if audit_present(args.name):
        LOG.error('Audit already exists, not registering')
        return

    #Write to audit file
    audit_cfg_args = {'name': args.name,
                      'conf': os.path.join(os.getcwd(), args.conf)}
    add_to_list('audit', **audit_cfg_args)
    LOG.info('Registered audit %s', args.name)


def register_repair(args):
    #TODO(praneshp) check for sanity (file exists, imp parameters exist, etc)
    LOG.warning('Registering repair script %s', args.name)

     #First check if you have all inputs
    if not (args.conf and args.name and args.engine):
        LOG.error('Need path to script and json')
        return

    #Check if this one is already present
    if repair_present(args.name):
        LOG.error('Repair script already exists, not registering')
        return

    #Write to audit file
    repair_cfg_args = {'name': args.name,
                       'conf': os.path.join(os.getcwd(), args.conf)}
    add_to_list('repair', **repair_cfg_args)
    LOG.info('Registered repair script %s', args.name)


def start_engine(args):
    # TODO(praneshp): for now, always look in entropy/cfg for config files.
    if not (args.name and args.audit_cfg and args.repair_cfg):
        LOG.error('Need name, audit_cfg, and repair_cfg')
        return
    engine_cfg = os.path.join(os.getcwd(), 'entropy', 'cfg', 'test.cfg')
    args.log_file = os.path.join(os.getcwd(), args.log_file)
    args.audit_cfg = os.path.join(os.getcwd(), args.audit_cfg)
    args.repair_cfg = os.path.join(os.getcwd(), args.repair_cfg)
    cfg = {'audit': args.audit_cfg, 'repair': args.repair_cfg}
    with open(engine_cfg, "w") as cfg_file:
        cfg_file.write(yaml.dump(cfg, canonical=False,
                                 default_flow_style=False,
                                 explicit_start=True))
    LOG.info('Wrote to engine cfg')
    global entropy_engine
    entropy_engine = Engine(args)


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
    register_audit_parser.add_argument('-e', dest='engine', action='store',
                                       help='Engine')
    register_audit_parser.set_defaults(func=register_audit)

    register_repair_parser =\
        subparsers.add_parser('register-repair',
                              help='Register a repair script')
    register_repair_parser.add_argument('-n', dest='name', action='store',
                                        help='Repair script name')
    register_repair_parser.add_argument('-c', dest='conf', action='store',
                                        help='Repair conf')
    register_repair_parser.add_argument('-e', dest='engine', action='store',
                                        help='Engine')
    register_repair_parser.set_defaults(func=register_repair)

    scheduler_parser = subparsers.add_parser('start-engine',
                                             help='Start an entropy engine')
    scheduler_parser.add_argument('-n', dest='name', help='Name')
    scheduler_parser.add_argument('-a', dest='audit_cfg',
                                  help='path to audit cfg')
    scheduler_parser.add_argument('-l', dest='log_file',
                                  help='log_file')
    scheduler_parser.add_argument('-r', dest='repair_cfg',
                                  help='path to repair cfg')
    scheduler_parser.set_defaults(func=start_engine)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    #TODO(praneshp): AMQP, json->yaml, reaction scripts(after amqp)

    logging.basicConfig(filename=globals.log_file,
                        level=logging.DEBUG)
    parse()
