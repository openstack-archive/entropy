#!/bin/bash
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4

set -o errexit
set -o xtrace


# print the top five process ids.
ps aux   | awk 'NR > 1 {print $1, $2, $12} ' | tail -n 5