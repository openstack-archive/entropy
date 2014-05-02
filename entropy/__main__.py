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
import logging
import os
import tempfile

import yaml

from engine import Engine
from entropy import utils

LOG = logging.getLogger(__name__)

# TODO(praneshp): Later, maybe find a way to make this configurable.
# Well known file where all engines are stored.
engine_cfg = os.path.join(tempfile.gettempdir(), 'engines.cfg')


def get_cfg_file(engine, script_type):
    cfg_key = {'audit': 'audit_cfg', 'repair': 'repair_cfg'}
    try:
        engine_config = dict(utils.load_yaml(engine_cfg).next())[engine]
        this_engine_cfg_file = engine_config['cfg']
        this_engine_cfg = dict(utils.load_yaml(this_engine_cfg_file).next())
        return this_engine_cfg[engine][cfg_key[script_type]]
    except KeyError:
        LOG.exception('Could not find engine/react script')
        return None


def add_to_list(engine, script_type, **kwargs):
    cfg_file = get_cfg_file(engine, script_type)
    if cfg_file is None:
        LOG.error('Could not find cfg file')
        return
    if utils.check_duplicate(kwargs.keys()[0], cfg_file):
        LOG.error('%s already exists, not registering', script_type)
        return
    with open(cfg_file, "a") as cfg:
        cfg.write(yaml.dump(kwargs, canonical=False,
                            default_flow_style=False,
                            explicit_start=True))
        return True


def register_audit(args):
    LOG.info('Registering audit script %s', args.name)

    # First check if you have all inputs
    if not (args.conf and args.name and args.engine):
        LOG.error('Need path to script, json and engine name')
        return

    # Write to audit file
    audit_cfg_args = {args.name:
                      {'conf': os.path.join(os.getcwd(), args.conf)}
                      }
    if add_to_list(args.engine, 'audit', **audit_cfg_args):
        LOG.info('Registered audit %s', args.name)


def register_repair(args):
    LOG.info('Registering repair script %s', args.name)

    # First check if you have all inputs
    if not (args.conf and args.name and args.engine):
        LOG.error('Need path to script, json and engine name')
        return

    # Write to audit file
    repair_cfg_args = {args.name:
                      {'conf': os.path.join(os.getcwd(), args.conf)}
                       }
    if add_to_list(args.engine, 'repair', **repair_cfg_args):
        LOG.info('Registered repair script %s', args.name)


def start_engine(args):
    # TODO(praneshp): for now, always look in entropy/cfg for config files.
    if not (args.name and args.engine_cfg):
        LOG.error('Need name and engine cfg')
        return
    cfg_data = dict(utils.load_yaml(args.engine_cfg).next())[args.name]
    cfg = {args.name:
           {'cfg': os.path.join(os.getcwd(), args.engine_cfg),
            'pid': os.getpid()
            }
           }
    with open(engine_cfg, "w") as cfg_file:
        cfg_file.write(yaml.dump(cfg, canonical=False,
                       default_flow_style=False,
                       explicit_start=True))
    LOG.info('Added %s to engine cfg', args.name)
    entropy_engine = Engine(args.name, **cfg_data)
    entropy_engine.run()


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
    scheduler_parser.add_argument('-c', dest='engine_cfg',
                                  help='path to engine cfg')
    scheduler_parser.set_defaults(func=start_engine)

    args = parser.parse_args()
    args.func(args)


def main():
    console_format = '%(filename)s %(lineno)s %(message)s'
    logging.basicConfig(format=console_format,
                        level=logging.INFO)
    parse()


if __name__ == '__main__':
    main()
