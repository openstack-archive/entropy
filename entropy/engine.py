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
import logging
import os

from entropy import utils

LOG = logging.getLogger(__name__)


class Engine(object):
    def __init__(self, name):
        # constants
        # TODO(praneshp): Hardcode for now, could/should be cmdline input
        self.script_repo = os.path.dirname(__file__)
        self.log_repo = os.path.join(os.getcwd(), 'entropy', 'logs')
        self.cfg_dir = os.path.join(self.script_repo, 'cfg')
        self.audit_cfg = os.path.join(self.cfg_dir, 'audit.cfg')
        self.repair_cfg = os.path.join(self.cfg_dir, 'repair.cfg')
        self.log_file = os.path.join(self.log_repo, 'entropy.log')
        self.max_workers = 8

        # engine variables
        self.name = name
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.running_audits = []
        self.running_repairs = []
        self.futures = []

        # TODO(praneshp): Look into how to do this with threadpoolexecutor?
        watchdog_thread = self.start_watchdog(self.cfg_dir)  # noqa
        watchdog_thread.join()

    def start_scheduler(self):
        pass

    def register_audit(self):
        pass

    def register_repair(self):
        pass

    # TODO(praneshp): For now, only addition of scripts. Take care of
    # deletion later

    def audit_modified(self):
        LOG.warning('Audit configuration changed')
#        self.all_futures.append(start_scripts('audit'))

    def repair_modified(self):
        LOG.warning('Repair configuration changed')
 #       start_scripts('repair')

    def start_watchdog(self, dir_to_watch):
        event_fn = {self.audit_cfg: self.audit_modified,
                    self.repair_cfg: self.repair_modified}
        return utils.watch_dir_for_change(dir_to_watch, event_fn)
