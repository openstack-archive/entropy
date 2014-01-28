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

import yaml

LOG = logging.getLogger(__name__)


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
