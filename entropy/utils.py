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

import logging
import os
import sys
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import yaml

from entropy import exceptions

LOG = logging.getLogger(__name__)


def get_filename_and_path(path):
    return os.path.dirname(path), os.path.basename(path)


def get_key_path():
    home_dir = os.path.expanduser("~")
    ssh_dir = os.path.join(home_dir, ".ssh")
    if not os.path.isdir(ssh_dir):
        return None
    for k in ('id_rsa', 'id_dsa'):
        path = os.path.join(ssh_dir, k)
        if os.path.isfile(path):
            return path
    return None


def load_yaml(filename):
    with open(filename, "rb") as fh:
        return yaml.safe_load(fh.read())


# importer functions.
# From cloudinit http://bazaar.launchpad.net/~cloud-init-dev/cloud-init/
# trunk/view/head:/cloudinit/importer.py

def import_module(module_name):
    __import__(module_name)
    return sys.modules[module_name]


# TODO(praneshp): return exception isntead
def find_module(base_name, search_paths, required_attrs=None):
    found_places = []
    if not required_attrs:
        required_attrs = []
    # NOTE(harlowja): translate the search paths to include the base name.
    real_paths = []
    for path in search_paths:
        real_path = []
        if path:
            real_path.extend(path.split("."))
        real_path.append(base_name)
        full_path = '.'.join(real_path)
        real_paths.append(full_path)
    LOG.info("Looking for modules %s that have attributes %s",
             real_paths, required_attrs)
    for full_path in real_paths:
        mod = None
        try:
            mod = import_module(full_path)
        except ImportError as e:
            LOG.debug("Failed at attempted import of '%s' due to: %s",
                      full_path, e)
        if not mod:
            continue
        found_attrs = 0
        for attr in required_attrs:
            if hasattr(mod, attr):
                found_attrs += 1
        if found_attrs == len(required_attrs):
            found_places.append(full_path)
    LOG.info("Found %s with attributes %s in %s", base_name,
             required_attrs, found_places)
    return found_places


class WatchdogHandler(FileSystemEventHandler):
    def __init__(self, event_fn):
        self.event_fn = event_fn

    def on_modified(self, event):
        if event.src_path in self.event_fn:
            self.event_fn[event.src_path]()
        else:
            LOG.error('no associated function for %s', event.src_path)


def watch_dir_for_change(dirs_to_watch, event_fn):
    event_handler = WatchdogHandler(event_fn)
    observer = Observer()
    for directory in dirs_to_watch:
        observer.schedule(event_handler, path=directory)
    observer.setDaemon(True)
    observer.start()
    return observer


def check_exists_and_enabled(name, cfg_file):
    engines = load_yaml(cfg_file)
    return engines and name in engines and engines[name]['enabled']


def check_exists_and_disabled(name, cfg_file):
    engines = load_yaml(cfg_file)
    return engines and name in engines and not engines[name]['enabled']


def purge_disabled(cfg_file):
    engines = load_yaml(cfg_file)
    final_engines = {}
    if not engines:
        return
    for engine in engines:
        if engines[engine]['enabled']:
            final_engines[engine] = engines[engine]
    if final_engines:
        write_yaml(final_engines, cfg_file, append=False)
    else:
        with open(cfg_file, 'w'):
            pass


def disable_engine(name, cfg_file):
    engines = load_yaml(cfg_file)
    if not engines:
        raise exceptions.NoEnginesException("No known engine!")
    if name not in engines:
        raise exceptions.NoSuchEngineException("No engines called %s!", name)
    engines[name]['enabled'] = False
    write_yaml(engines, cfg_file, append=False)
    return engines[name]['pid']


def reset_logger(log):
    if not log:
        return
    handlers = list(log.handlers)
    for h in handlers:
        h.flush()
        h.close()
        log.removeHandler(h)
    log.setLevel(logging.NOTSET)
    log.addHandler(logging.NullHandler())


def write_yaml(data, filename, append=True):
    mode = "a" if append else "w"
    with open(filename, mode) as cfg_file:
        cfg_file.write(yaml.safe_dump(data,
                                      default_flow_style=False,
                                      canonical=False))


def wallclock():
    # NOTE(harlowja): made into a function so that this can be easily mocked
    # out if we want to alter time related functionality (for testing
    # purposes).
    return time.time()


# From taskflow:
# https://github.com/openstack/taskflow/blob/master/taskflow/utils/misc.py
class StopWatch(object):
    """A simple timer/stopwatch helper class.

    Inspired by: apache-commons-lang java stopwatch.

    Not thread-safe.
    """
    _STARTED = 'STARTED'
    _STOPPED = 'STOPPED'

    def __init__(self, duration=None):
        self._duration = duration
        self._started_at = None
        self._stopped_at = None
        self._state = None

    def start(self):
        if self._state == self._STARTED:
            return self
        self._started_at = wallclock()
        self._stopped_at = None
        self._state = self._STARTED
        return self

    def elapsed(self):
        if self._state == self._STOPPED:
            return float(self._stopped_at - self._started_at)
        elif self._state == self._STARTED:
            return float(wallclock() - self._started_at)
        else:
            raise RuntimeError("Can not get the elapsed time of an invalid"
                               " stopwatch")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        try:
            self.stop()
        except RuntimeError:
            pass
        # NOTE(harlowja): don't silence the exception.
        return False

    def leftover(self):
        if self._duration is None:
            raise RuntimeError("Can not get the leftover time of a watch that"
                               " has no duration")
        if self._state != self._STARTED:
            raise RuntimeError("Can not get the leftover time of a stopwatch"
                               " that has not been started")
        end_time = self._started_at + self._duration
        return max(0.0, end_time - wallclock())

    def expired(self):
        if self._duration is None:
            return False
        if self.elapsed() > self._duration:
            return True
        return False

    def resume(self):
        if self._state == self._STOPPED:
            self._state = self._STARTED
            return self
        else:
            raise RuntimeError("Can not resume a stopwatch that has not been"
                               " stopped")

    def stop(self):
        if self._state == self._STOPPED:
            return self
        if self._state != self._STARTED:
            raise RuntimeError("Can not stop a stopwatch that has not been"
                               " started")
        self._stopped_at = wallclock()
        self._state = self._STOPPED
        return self


def create_files(list_of_files):
    if not list_of_files:
        return
    for filename in list_of_files:
        if not os.path.isfile(filename):
            with open(filename, 'w'):
                pass
