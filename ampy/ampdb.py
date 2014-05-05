#!/usr/bin/env python
# -*- coding: ascii -*-

"""
ampdb handles database connections and working with AMP data
"""

import ampy.netevmon
import ampy.nntsc


def create_netevmon_engine(host, dbname, pwd, user, port=None):
    return ampy.netevmon.Connection(host, dbname, pwd, user, port)

# Use this engine to get at core NNTSC information, e.g. the collection list
def create_nntsc_engine(host, port, ampconfig=None, viewconfig=None):
    return ampy.nntsc.Connection(host, port, ampconfig, viewconfig)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
