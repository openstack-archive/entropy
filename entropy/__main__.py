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
from concurrent.futures import ProcessPoolExecutor
import datetime
import logging
import os
import sys

import croniter
import pause
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import yaml

sys.path.insert(0, os.path.join(os.path.abspath(os.pardir)))
sys.path.insert(0, os.path.abspath(os.getcwd()))

from entropy import globals
from entropy import utils

LOG = logging.getLogger(__name__)
running_audits = []
running_repairs = []
executor = ProcessPoolExecutor(max_workers=globals.MAX_WORKERS)


def run_scheduler(args):
    LOG.info('Starting Scheduler')
    # Start watchdog thread. If any new audit/react scripts are added,
    # detect and add.
    # TODO(praneshp): Look into how to do this with threadpoolexecutor?
    watchdog_thread = start_watchdog(globals.CFG_DIR)

    # Start react and audit scripts.
    react_futures = start_scripts('repair')  # noqa
    audit_futures = start_scripts('audit')  # noqa

    watchdog_thread.join()

def start_scripts(script_type):
    # TODO(praneshp):audit threads can use thread pool, reacts can use process?

    if script_type == 'audit':
        (running_scripts, setup_func) = (running_audits, setup_audit)
        cfg = globals.AUDIT_CFG
    elif script_type == 'repair':
        (running_scripts, setup_func) = (running_repairs, setup_react)
        cfg = globals.REPAIR_CFG

    scripts = utils.load_yaml(cfg)
    futures = []

    for script in scripts:
        if script['name'] not in running_scripts:
            futures.append(setup_func(script))

    LOG.warning('Running %s scripts %s', script_type,
                ', '.join(running_scripts))
    return futures


# TODO(praneshp): For now, only addition of scripts. Take care of
# deletion later

def audit_modified():
    LOG.warning('Audit configuration changed')
    new_audits = start_scripts('audit')
    [t.join() for t in new_audits]


def repair_modified():
    LOG.warning('Repair configuration changed')
    start_scripts('repair')


class WatchdogHandler(FileSystemEventHandler):
    def on_modified(self, event):
        LOG.warning('Monitored file changed %s', event.src_path)
        if event.src_path == globals.AUDIT_CFG:
            audit_modified()
        elif event.src_path == globals.REPAIR_CFG:
            repair_modified()


def start_watchdog(dir_to_watch):
    return watch_dir_for_change(dir_to_watch)


def watch_dir_for_change(dir_to_watch):
    event_handler = WatchdogHandler()
    observer = Observer()
    observer.schedule(event_handler, path=dir_to_watch, recursive=True)
    observer.start()
    return observer


def add_to_list(script_type, **kwargs):
    if script_type == 'audit':
        cfg_file = globals.AUDIT_CFG
    else:
        cfg_file = globals.REPAIR_CFG
    with open(cfg_file, "a") as cfg:
        cfg.write(yaml.dump(kwargs, canonical=False,
                            default_flow_style=False,
                            explicit_start=True))


def setup_audit(script):
    LOG.warning('Setting up audit script %s', script['name'])

    # Now pick out relevant info
    data = dict(utils.load_yaml(script['conf']).next())
    # stuff for the message queue
    mq_args = {'mq_host': data['mq_host'],
               'mq_port': data['mq_port'],
               'mq_user': data['mq_user'],
               'mq_password': data['mq_password']}

    # general stuff for the audit module
    # TODO(praneshp): later, fix to send only one copy of mq_args
    kwargs = data
    kwargs['mq_args'] = mq_args

    # add this job to list of running audits
    running_audits.append(script['name'])

    # start a process for this audit script
    future = executor.submit(start_audit, **kwargs)
    return future


def setup_react(script):
    LOG.warning('Setting up reactor %s', script['name'])

    # Pick out relevant info
    data = dict(utils.load_yaml(script['conf']).next())
    react_script = data['script']

    available_modules = utils.find_module(react_script, ['repair'])
    LOG.info('Found these modules: %s', available_modules)
    if not available_modules:
        LOG.error('No module to load')
    else:
        imported_module = utils.import_module(available_modules[0])
        kwargs = data
        kwargs['conf'] = script['conf']

        # add this job to list of running audits
        running_repairs.append(script['name'])

        future = executor.submit(imported_module.main, **kwargs)
        return future


def run_audit(**kwargs):
    # Put a message on the mq
    #TODO(praneshp): this should be the path with register-audit
    #TODO(praneshp): The whole logic in this function should be in
    # try except blocks
    available_modules = utils.find_module(kwargs['module'], ['audit'])
    LOG.info('Found these modules: %s', available_modules)
    if not available_modules:
        LOG.error('No module to load')
    else:
        imported_module = utils.import_module(available_modules[0])
        audit_obj = imported_module.Audit()
        try:
            audit_obj.send_message(**kwargs)
        except Exception as e:
            LOG.error(e)


def start_audit(**kwargs):
    LOG.info("Starting audit for %s", kwargs['name'])
    now = datetime.datetime.now()
    schedule = kwargs['schedule']
    cron = croniter.croniter(schedule, now)
    next_iteration = cron.get_next(datetime.datetime)
    while True:
        LOG.warning('Next call at %s', next_iteration)
        pause.until(next_iteration)
        run_audit(**kwargs)
        next_iteration = cron.get_next(datetime.datetime)


def check_duplicate(name, cfg_file):
    with open(cfg_file, 'r') as cfg:
        scripts = utils.load_yaml(cfg)
        names = [script['name'] for script in scripts]
        if name in names:
            return True
    return False


def repair_present(name):
    return check_duplicate(name, globals.REPAIR_CFG)


def audit_present(name):
    return check_duplicate(name, globals.AUDIT_CFG)


def register_audit(args):
    #TODO(praneshp) check for sanity (file exists, imp parameters exist, etc)
    LOG.warning('Registering audit script %s', args.name)

    #First check if you have all inputs
    if not (args.conf or args.name):
        LOG.error('Need path to script and json')
        return

    #Check if this one is already present
    if audit_present(args.name):
        LOG.error('Audit already exists, not registering')
        return

    #Write to audit file
    audit_cfg_args = {'name': args.name,
                      'conf': args.conf}
    add_to_list('audit', **audit_cfg_args)
    LOG.info('Registered audit %s', args.name)


def register_repair(args):
    #TODO(praneshp) check for sanity (file exists, imp parameters exist, etc)
    LOG.warning('Registering repair script %s', args.name)

     #First check if you have all inputs
    if not (args.conf or args.name):
        LOG.error('Need path to script and json')
        return

    #Check if this one is already present
    if repair_present(args.name):
        LOG.error('Repair script already exists, not registering')
        return

    #Write to audit file
    repair_cfg_args = {'name': args.name,
                       'conf': args.conf}
    add_to_list('repair', **repair_cfg_args)
    LOG.info('Registered repair script %s', args.name)


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

    logging.basicConfig(filename=globals.log_file,
                        level=logging.INFO)
    parse()
