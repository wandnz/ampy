#!/usr/bin/env python
# -*- coding: ascii -*-

"""
ampdb handles database connections and working with AMP data
"""

import engine


def create():
    """ Creates and returns instance of an ampdb connection """
    instance = engine.Connection()
    instance._connect()
    return instance

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
