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

from engine import Engine
from entropy import utils

LOG = logging.getLogger(__name__)

# TODO(praneshp): Later, maybe find a way to make this configurable.
# Well known file where all engines are stored.
engine_cfg = os.path.join(tempfile.gettempdir(), 'engines.cfg')


def _get_backend_from_engine(engine):
    try:
        engine_config = dict(utils.load_yaml(engine_cfg))[engine]
        this_engine_cfg_file = engine_config['cfg']
        this_engine_cfg = dict(utils.load_yaml(this_engine_cfg_file))
        return Engine.get_backend(this_engine_cfg[engine]['backend'],
                                  this_engine_cfg[engine])
    except KeyError:
        LOG.exception("Could not find engine's cfg script")


def _add_to_list(engine, script_type, script_name, **script_args):
    backend = _get_backend_from_engine(engine)
    if backend.check_script_exists(script_type, script_name):
        LOG.error('%s already exists, not registering', script_type)
        return False
    try:
        data = {
            script_name: script_args
        }
        backend.add_script(script_type, data)
        return True
    except KeyError:
        LOG.exception("No %s script called %s", script_type, script_name)
    except Exception:
        LOG.exception("Could not register %s script %s", script_type,
                      script_name)
        return False


def _remove_from_list(engine, script_type, script_name):
    backend = _get_backend_from_engine(engine)
    try:
        backend.remove_script(script_type, script_name)
    except Exception:
        LOG.exception("Could not remove %s script %s",
                      script_type, script_name)


def register_audit(args):
    LOG.info('Registering audit script %s', args.name)

    # First check if you have all inputs
    if not (args.conf and args.name and args.engine):
        LOG.error('Need path to script, json and engine name')
        return

    # Write to audit file
    audit_cfg_args = {'cfg': os.path.join(os.getcwd(), args.conf)}
    if _add_to_list(args.engine, 'audit', args.name, **audit_cfg_args):
        LOG.info('Registered audit %s', args.name)


def unregister_audit(args):
    LOG.info('Unregistering audit script %s', args.name)
    if not args.name and args.engine:
        LOG.error('Need a audit name and engine to unregister')
        return
    _remove_from_list(args.engine, 'audit', args.name)


def register_repair(args):
    LOG.info('Registering repair script %s', args.name)

    # First check if you have all inputs
    if not (args.conf and args.name and args.engine):
        LOG.error('Need path to script, json and engine name')
        return

    # Write to repair file
    repair_cfg_args = {'cfg': os.path.join(os.getcwd(), args.conf)}
    if _add_to_list(args.engine, 'repair', args.name, **repair_cfg_args):
        LOG.info('Registered repair script %s', args.name)


def unregister_repair(args):
    LOG.info('Unregistering repair script %s', args.name)
    if not args.name and args.engine:
        LOG.error('Need a repair name and engine to unregister')
        return
    _remove_from_list(args.engine, 'repair', args.name)


def start_engine(args):
    if not (args.name and args.engine_cfg):
        LOG.error('Need name and engine cfg')
        return
    utils.create_files([engine_cfg])
    if args.purge:
        utils.purge_disabled(engine_cfg)
    if utils.check_exists_and_enabled(args.name, engine_cfg):
        LOG.error("An engine of the same name %s is already "
                  "registered and running", args.name)
        return
    if utils.check_exists_and_disabled(args.name, engine_cfg):
        LOG.error("And engine of the same name %s is already "
                  "registered, but disabled. Run with purge?", args.name)
        return
    try:
        cfg_data = dict(utils.load_yaml(args.engine_cfg))[args.name]
        cfg = {
            args.name: {
                'cfg': os.path.join(os.getcwd(), args.engine_cfg),
                'pid': os.getpid(),
                'backend': cfg_data['backend'],
                'enabled': True
            }
        }
        utils.write_yaml(cfg, engine_cfg)
        LOG.info('Added %s to engine cfg', args.name)
        entropy_engine = Engine(args.name, **cfg_data)
        entropy_engine.run()
    except Exception:
        LOG.exception("Could not start engine %s", args.name)
        return


def stop_engine(args):
    LOG.info("Stopping engine %s", args.name)
    # Grab engine config file, set our engine to disabled
    utils.disable_engine(args.name, engine_cfg)


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

    unregister_audit_parser =\
        subparsers.add_parser('unregister-audit',
                              help='Unregister a repair script')
    unregister_audit_parser.add_argument('-n', dest='name', action='store',
                                         help='Repair script name')
    unregister_audit_parser.add_argument('-e', dest='engine', action='store',
                                         help='Engine')
    unregister_audit_parser.set_defaults(func=unregister_audit)
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

    unregister_repair_parser = \
        subparsers.add_parser('unregister-repair',
                              help='Unregister a repair script')
    unregister_repair_parser.add_argument('-n', dest='name', action='store',
                                          help='Repair script name')
    unregister_repair_parser.add_argument('-e', dest='engine', action='store',
                                          help='Engine')
    unregister_repair_parser.set_defaults(func=unregister_repair)

    start_engine_parser = subparsers.add_parser('start-engine',
                                                help='Start an entropy engine')
    start_engine_parser.add_argument('-n', dest='name', help='Name')
    start_engine_parser.add_argument('-c', dest='engine_cfg',
                                     help='path to engine cfg')
    start_engine_parser.add_argument('-p', dest='purge', action='store_true',
                                     help='Purge disabled engines')
    start_engine_parser.set_defaults(func=start_engine)

    stop_engine_parser = subparsers.add_parser('stop-engine',
                                               help='Stop an entropy engine')
    stop_engine_parser.add_argument('-n', dest='name',
                                    help="Name of engine to stop")
    stop_engine_parser.set_defaults(func=stop_engine)

    args = parser.parse_args()
    args.func(args)


def main():
    console_format = '%(filename)s %(lineno)s %(message)s'
    logging.basicConfig(format=console_format,
                        level=logging.INFO)
    parse()


if __name__ == '__main__':
    main()
