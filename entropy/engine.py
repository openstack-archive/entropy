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

import collections
import datetime
import logging
import operator
import os
import tempfile

from concurrent import futures as cf
import croniter
from kombu import Exchange
from kombu import Queue
import pause
import six
from stevedore import driver

from entropy import exceptions
from entropy import states
from entropy import utils
import imp
LOG = logging.getLogger(__name__)


class Engine(object):
    def __init__(self, name, **cfg_data):
        utils.reset_logger(logging.getLogger())
        Engine.set_logger(**cfg_data)
        # constants
        # Well known file where all engines are stored.
        self.engine_cfg = os.path.join(tempfile.gettempdir(), 'engines.cfg')
        # TODO(praneshp): Hardcode for now, could/should be cmdline input
        self.max_workers = 8
        self._engine_cfg_data = cfg_data
        self.audit_type = 'audit'
        self.repair_type = 'repair'
        self.entropy_exchange = Exchange('entropy_exchange', type='fanout')
        self.known_queues = []
        # engine variables
        self.name = name
        self.audit_cfg = cfg_data['audit_cfg']
        self.repair_cfg = cfg_data['repair_cfg']
        self.serializer_schedule = cfg_data['serializer_schedule']
        self.engine_timeout = cfg_data['engine_timeout']
        # TODO(praneshp): Assuming cfg files are in 1 dir. Change later
        self._backend = cfg_data['backend']
        self._backend_driver = self.get_backend(self._backend,
                                                self._engine_cfg_data)
        self.cfg_dir = os.path.dirname(self.audit_cfg)
        self.log_file = cfg_data['log_file']
        self.executor = cf.ThreadPoolExecutor(max_workers=self.max_workers)
        self.running_audits = []
        self.running_repairs = []
        self.futures = []
        self.run_queue = collections.deque()
        # Private variables
        self._watchdog_event_fn = {self.repair_cfg: self.repair_modified,
                                   self.engine_cfg: self.engine_disabled}
        # Private variables to keep track of repair scripts.
        self._repairs = []
        self._known_routing_keys = {}

        # Watchdog-related variables
        self._watchdog_thread = None

        # Serializer related variables
        self._serializer = None

        # State related variables
        self._state = states.ENABLED

        LOG.info('Created engine obj %s', self.name)

    # TODO(praneshp): Move to utils?
    @staticmethod
    def set_logger(**cfg_data):
        # Set the logger
        LOG.handlers = []
        log_to_file = logging.FileHandler(cfg_data['log_file'])
        log_to_file.setLevel(logging.INFO)
        log_format = logging.Formatter(cfg_data['log_format'])
        log_to_file.setFormatter(log_format)
        LOG.addHandler(log_to_file)
        LOG.propagate = False

    @staticmethod
    def get_backend(backend, cfg_data):
        backend = driver.DriverManager(
            namespace='entropy.backend',
            name=backend,
            invoke_on_load=True,
            invoke_args=(cfg_data,),
        )
        return backend.driver

    def run(self):
        LOG.info('Starting Scheduler for %s', self.name)
        self.start_scheduler()

    def start_scheduler(self):
        if not self._serializer:
            self._serializer = self.executor.submit(self.start_serializer)
            self.futures.append(self._serializer)

        # Start react scripts.
        self.futures.extend(self.start_react_scripts(
            self._get_react_scripts()))

        scheduler = self.executor.submit(self.schedule)
        self.futures.append(scheduler)

        # watchdog
        self._watchdog_thread = self.start_watchdog()
        self._watchdog_thread.join()

    def schedule(self):
        while self._state == states.ENABLED:
            (next_time, next_jobs) = self.wait_next(self.engine_timeout)
            # NOTE(praneshp): here, call a function that will wait till next
            # time and call next_jobs,
            if next_jobs:
                self.setup_audit(next_time, next_jobs)

    def wait_next(self, timeout=None):
        watch = None
        next_jobs = []
        if timeout is not None:
            watch = utils.StopWatch(duration=float(timeout))
            watch.start()
        try:
            while True:
                if not self.run_queue:
                    if watch and watch.expired():
                        raise exceptions.TimeoutException(
                            "Died at %s after waiting for audits to arrive "
                            "for %s" % (utils.wallclock(), watch.elapsed()))
                else:
                    # Grab all the jobs for the next time.
                    next_jobs.append(self.run_queue.popleft())
                    next_time = next_jobs[0]['time']
                    l = len(self.run_queue)
                    for i in xrange(l):
                        if self.run_queue[0]['time'] == next_time:
                            next_jobs.append(self.run_queue.popleft())
                    return next_time, next_jobs
        except exceptions.TimeoutException as te:
            LOG.info("%s", te.message)
            return None, []
        except Exception:
            LOG.exception("Something went wrong")
            return None, []

    def start_serializer(self):
        schedule = self.serializer_schedule
        now = datetime.datetime.now()
        cron = croniter.croniter(schedule, now)
        next_iteration = cron.get_next(datetime.datetime)
        while self._state == states.ENABLED:
            LOG.info('It is %s, next serializer at %s', now, next_iteration)
            pause.until(next_iteration)
            now = datetime.datetime.now()
            next_iteration = cron.get_next(datetime.datetime)
            if self._state == states.ENABLED:
                try:
                    self.run_serializer(next_iteration, now)
                except exceptions.SerializerException:
                    LOG.exception("Could not run serializer")

    def run_serializer(self, next_iteration, current_time):
        LOG.info("Running serializer for %s at %s", self.name, current_time)
        audits = self._backend_driver.get_audits()
        schedules = {}
        if not audits:
            LOG.info('No audits to run, returning')
            return
        try:
            for audit_name in audits:
                audit_cfg = self._backend_driver.audit_cfg_from_name(
                    audit_name)
                schedules[audit_name] = audit_cfg['schedule']
            new_additions = []

            for key in six.iterkeys(schedules):
                sched = schedules[key]
                now = datetime.datetime.now()
                cron = croniter.croniter(sched, now)
                while True:
                    next_call = cron.get_next(datetime.datetime)
                    if next_call > next_iteration:
                        break
                    new_additions.append({'time': next_call, 'name': key})

            new_additions.sort(key=operator.itemgetter('time'))

            # NOTE(praneshp): Protect this operation with a state check, so in
            # case of race conditions no extra audit scripts are added.
            if self._state == states.ENABLED:
                self.run_queue.extend(new_additions)
            LOG.info("Run queue till %s is %s", next_iteration, self.run_queue)
            LOG.info("Repair scripts at %s: %s", next_iteration, self._repairs)
        except Exception as e:
            raise exceptions.SerializerException(
                "Could not run serializer for %s at %s" %
                (self.name, current_time), e)

    def engine_disabled(self):
        engine_config = dict(utils.load_yaml(self.engine_cfg))[self.name]
        if not engine_config['enabled']:
            self.stop_engine()

    def stop_engine(self):
        LOG.info("Stopping engine %s", self.name)
        # Set state to stop, which will stop serializers
        self._state = states.DISABLED
        # Clear run queue
        LOG.info("Clearing audit run queue for %s", self.name)
        self.run_queue.clear()
        # Stop all repairs - not yet implemented
        # Stop watchdog monitoring
        LOG.info("Stopping watchdog for %s", self.name)
        self._watchdog_thread.stop()

    def repair_modified(self):
        LOG.info('Repair configuration changed')
        repairs = self._get_react_scripts()
        new_repairs = {}
        repairs_to_delete = []
        for repair in repairs:
            if repair not in self.running_repairs:
                new_repairs[repair] = repairs[repair]
        if self.running_repairs:
            for repair in self.running_repairs:
                if repair not in repairs:
                    repairs_to_delete.append(repair)
        LOG.info('will add new repairs: %s', new_repairs)
        LOG.info('will nuke repairs: %s', repairs_to_delete)
        self.futures.extend(self.start_react_scripts(new_repairs))

    def start_watchdog(self):
        LOG.debug('Watchdog mapping is: ', self._watchdog_event_fn)
        dirs_to_watch = [utils.get_filename_and_path(x)[0] for x in
                         self.engine_cfg, self.repair_cfg]
        return utils.watch_dir_for_change(dirs_to_watch,
                                          self._watchdog_event_fn)

    def setup_audit(self, execution_time, audit_list):
        try:
            pause.until(execution_time)
            # Only proceed if engine is running, i.e in enabled state.
            if self._state != states.ENABLED:
                LOG.info("%s is disabled, so not running audits at %s",
                         self.name, execution_time)
                return
            LOG.info("Time: %s, Starting %s", execution_time, audit_list)
            audit_futures = []
            for audit in audit_list:
                audit_name = audit['name']
                audit_cfg = self._backend_driver.audit_cfg_from_name(
                    audit_name)
                future = self.executor.submit(self.run_audit,
                                              audit_name=audit_name,
                                              **audit_cfg)
                audit_futures.append(future)
            if audit_futures:
                self.futures.extend(audit_futures)
        except Exception:
            LOG.exception("Could not run all audits in %s at %s",
                          audit_list, execution_time)

    def _get_react_scripts(self):
        repairs = self._backend_driver.get_repairs()
        return repairs

    def start_react_scripts(self, repairs):
        futures = []
        if repairs:
            for script in repairs:
                if script not in self.running_repairs:
                    future = self.setup_react(script, **repairs[script])
                    if future is not None:
                        futures.append(future)
        LOG.info('Running repair scripts %s', ', '.join(self.running_repairs))
        return futures

    def setup_react(self, script, **script_args):
        LOG.info('Setting up reactor %s', script)

        # Pick out relevant info
        data = self._backend_driver.repair_cfg_from_name(script)
        react_script = data['script']
        search_path, reactor = utils.get_filename_and_path(react_script)
        available_modules = imp.find_module(reactor, [search_path])
        LOG.debug('Found these modules: %s', available_modules)
        try:
            # create any queues this react script wants, add it to a list
            # of known queues
            message_queue = Queue(self.name,
                                  self.entropy_exchange,
                                  data['routing_key'])
            if message_queue not in self.known_queues:
                self.known_queues.append(message_queue)
            self._known_routing_keys[script] = data['routing_key']
            kwargs = data
            kwargs['name'] = script
            kwargs['conf'] = script_args['cfg']
            kwargs['exchange'] = self.entropy_exchange
            kwargs['message_queue'] = message_queue
            # add this job to list of running repairs
            self.running_repairs.append(script)
            imported_module = imp.load_module(react_script, *available_modules)
            future = self.executor.submit(imported_module.main, **kwargs)
            self._repairs.append(future)
            return future
        except Exception:
            LOG.exception("Could not setup %s", script)
            return None

    def run_audit(self, audit_name, **audit_cfg):
        # general stuff for the audit module
        # TODO(praneshp): later, fix to send only one copy of mq_args
        mq_args = {'mq_host': audit_cfg['mq_host'],
                   'mq_port': audit_cfg['mq_port'],
                   'mq_user': audit_cfg['mq_user'],
                   'mq_password': audit_cfg['mq_password']}
        audit_cfg['mq_args'] = mq_args
        audit_cfg['exchange'] = self.entropy_exchange
        audit_cfg['name'] = audit_name
        # Put a message on the mq
        # TODO(praneshp): this should be the path with register-audit
        try:
            audit_script = audit_cfg['module']
            search_path, auditor = utils.get_filename_and_path(audit_script)
            available_modules = imp.find_module(auditor, [search_path])
            LOG.debug('Found these modules: %s', available_modules)
            imported_module = imp.load_module(audit_script, *available_modules)
            audit_obj = imported_module.Audit(**audit_cfg)
            audit_obj.send_message(**audit_cfg)
        except Exception:
                LOG.exception('Could not run audit %s', audit_name)
