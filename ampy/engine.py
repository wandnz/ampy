#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Connects to an AMP database and interacts with it
"""


class Connection(object):
    def __init__(self):
        """ Initialises an AMP connection """
        print "I'm a new AMP connection"

    def connect(self):
        """ Connects to AMP """
        print "Connected to AMP"

    def get(self):
        """ Fetches data from the connection """
        print "Got some data"

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
