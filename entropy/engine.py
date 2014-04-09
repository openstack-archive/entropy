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

from concurrent.futures import ThreadPoolExecutor
import datetime
import logging
import os

import croniter
from kombu import Exchange
import pause

from entropy import utils

LOG = logging.getLogger(__name__)


class Engine(object):
    def __init__(self, name, **cfg_data):
        # constants
        # TODO(praneshp): Hardcode for now, could/should be cmdline input
        self.max_workers = 8
        self.audit_type = 'audit'
        self.repair_type = 'repair'
        self.entropy_exchange = Exchange('entropy_exchage', type='fanout')
        # engine variables
        self.name = name
        self.audit_cfg = cfg_data['audit_cfg']
        self.repair_cfg = cfg_data['repair_cfg']
        # TODO(praneshp): Assuming cfg files are in 1 dir. Change later
        self.cfg_dir = os.path.dirname(self.audit_cfg)
        self.log_file = cfg_data['log_file']
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.running_audits = []
        self.running_repairs = []
        self.futures = []
        LOG.info('Created engine obj %s', self.name)

    def run(self):
        LOG.info('Starting Scheduler for %s', self.name)
        self.start_scheduler()

    def start_scheduler(self):
        # Start watchdog thread, which will detect any new audit/react scripts
        # TODO(praneshp): Look into how to do this with threadpoolexecutor?
        watchdog_thread = self.start_watchdog(self.cfg_dir)  # noqa

        # Start react and audit scripts.
        self.futures.append(self.start_scripts('repair'))
        self.futures.append(self.start_scripts('audit'))
        watchdog_thread.join()

    # TODO(praneshp): For now, only addition of scripts. Take care of
    # deletion later
    def audit_modified(self):
        LOG.info('Audit configuration changed')
        self.futures.append(self.start_scripts('audit'))

    def repair_modified(self):
        LOG.info('Repair configuration changed')
        self.futures.append(self.start_scripts('repair'))

    def start_watchdog(self, dir_to_watch):
        event_fn = {self.audit_cfg: self.audit_modified,
                    self.repair_cfg: self.repair_modified}
        LOG.info(event_fn)
        return utils.watch_dir_for_change(dir_to_watch, event_fn)

    def start_scripts(self, script_type):
        if script_type == 'audit':
            running_scripts = self.running_audits
            setup_func = self.setup_audit
            cfg = self.audit_cfg
        elif script_type == 'repair':
            running_scripts = self.running_repairs
            setup_func = self.setup_react
            cfg = self.repair_cfg
        else:
            LOG.error('Unknown script type %s', script_type)
            return

        scripts = utils.load_yaml(cfg)
        futures = []

        for script in scripts:
            if script['name'] not in running_scripts:
                futures.append(setup_func(script))
        LOG.info('Running %s scripts %s', script_type,
                 ', '.join(running_scripts))
        return futures

    def setup_react(self, script):
        LOG.info('Setting up reactor %s', script['name'])

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
            self.running_repairs.append(script['name'])

            future = self.executor.submit(imported_module.main, **kwargs)
            return future

    def setup_audit(self, script):
        LOG.info('Setting up audit script %s', script['name'])

        # add this job to list of running audits
        self.running_audits.append(script['name'])

        # start a process for this audit script
        future = self.executor.submit(self.start_audit, script)
        return future

    def start_audit(self, script):
        LOG.info("Starting audit for %s", script['name'])
        data = dict(utils.load_yaml(script['conf']).next())
        schedule = data['schedule']
        now = datetime.datetime.now()
        cron = croniter.croniter(schedule, now)
        next_iteration = cron.get_next(datetime.datetime)
        while True:
            LOG.info('Next call at %s', next_iteration)
            pause.until(next_iteration)
            self.run_audit(script)
            next_iteration = cron.get_next(datetime.datetime)

    def run_audit(self, script):
        # Read the conf file
        data = dict(utils.load_yaml(script['conf']).next())
        # general stuff for the audit module
        # TODO(praneshp): later, fix to send only one copy of mq_args
        mq_args = {'mq_host': data['mq_host'],
                   'mq_port': data['mq_port'],
                   'mq_user': data['mq_user'],
                   'mq_password': data['mq_password']}
        kwargs = data
        kwargs['mq_args'] = mq_args
        kwargs['exchange'] = self.entropy_exchange
        # Put a message on the mq
        #TODO(praneshp): this should be the path with register-audit
        try:
            available_modules = utils.find_module(kwargs['module'], ['audit'])
            LOG.info('Found these modules: %s', available_modules)
            imported_module = utils.import_module(available_modules[0])
            audit_obj = imported_module.Audit(**kwargs)
            audit_obj.send_message(**kwargs)
        except Exception:
                LOG.exception('Could not run audit %s', kwargs['name'])
