#!/usr/bin/env python
# -*- coding: ascii -*-

"""
ampdb handles database connections and working with AMP data
"""

import ampy.engine
import ampy.smokeping
import ampy.muninbytes
import ampy.netevmon
import ampy.nntsc


def create():
    """ Creates and returns instance of an ampdb connection """
    return ampy.engine.Connection()

def create_smokeping_engine(host, port):
    return ampy.smokeping.Connection(host, port)

def create_muninbytes_engine(host, port):
    return ampy.muninbytes.Connection(host, port)

def create_netevmon_engine(host, dbname, pwd):
    return ampy.netevmon.Connection(host, dbname, pwd)

# Use this engine to get at core NNTSC information, e.g. the collection list
def create_nntsc_engine(host, port):
    return ampy.nntsc.Connection(host, port)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
