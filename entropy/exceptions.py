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


class EntropyException(Exception):
    """Base class for exceptions emitted from entropy."""
    def __init__(self, message, cause=None):
        super(EntropyException, self).__init__(message)
        self._cause = cause

    @property
    def cause(self):
        return self._cause


class TimeoutException(EntropyException):
    """Exceptions because of timeouts, eg. when the job queue has been empty
    really long.
    """


class EngineStoppedException(EntropyException):
    """Exception raised when operations are performed against an engine that
    is shutdown.
    """


class NoSuchEngineException(EntropyException):
    """Exception raised when performing operations on a non-existent engine.
    """


class NoEnginesException(EntropyException):
    """Exception raised when there are no known engines."""


class SerializerException(EntropyException):
    """Exception raised when the serializer fails."""

