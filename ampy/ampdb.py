#!/usr/bin/env python
# -*- coding: ascii -*-

"""
ampdb handles database connections and working with AMP data
"""

import ampy.engine


def create():
    """ Creates and returns instance of an ampdb connection """
    return ampy.engine.Connection()

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
