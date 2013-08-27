#!/usr/bin/env python
# -*- coding: ascii -*-

"""
ampdb handles database connections and working with AMP data
"""

import ampy.netevmon
import ampy.nntsc


def create_netevmon_engine(host, dbname, pwd, user):
    return ampy.netevmon.Connection(host, dbname, pwd, user)

# Use this engine to get at core NNTSC information, e.g. the collection list
def create_nntsc_engine(host, port, ampconfig=None):
    return ampy.nntsc.Connection(host, port, ampconfig)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
