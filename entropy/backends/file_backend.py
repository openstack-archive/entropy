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
from entropy.backends import base
from entropy import utils


class FileBackend(base.Backend):
    """A directory based backend."""
    def __init__(self, conf):
        super(FileBackend, self).__init__(conf)
        self._audit_cfg = conf['audit_cfg']
        self._repair_cfg = conf['repair_cfg']
        self.setup()

    def setup(self):
        utils.create_files([self._audit_cfg, self._repair_cfg])

    def open(self):
        pass

    def close(self):
        pass

    def get_audits(self):
        audits = utils.load_yaml(self._audit_cfg)
        return audits

    def get_repairs(self):
        repairs = utils.load_yaml(self._repair_cfg)
        return repairs

    def audit_cfg_from_name(self, name):
        audits = self.get_audits()
        conf = audits[name]['cfg']
        audit_cfg = dict(utils.load_yaml(conf))
        return audit_cfg

    def repair_cfg_from_name(self, name):
        repairs = self.get_repairs()
        conf = repairs[name]['cfg']
        repair_cfg = dict(utils.load_yaml(conf))
        return repair_cfg
