#!/usr/bin/env python

import sys, string

class MuninbytesParser(object):

    def __init__(self):
        self.streams = {}
        self.interfaces = {}
        self.directions = {}
        self.switches = {}

    def add_stream(self, s):
        
        self.switches[s['switch']] = 1
        
        if s['switch'] in self.interfaces:
            self.interfaces[s['switch']][s['interfacelabel']] = 1
        else:
            self.interfaces[s['switch']] = {s['interfacelabel']:1}

        if (s['switch'], s['interfacelabel']) in self.directions:
            self.directions[(s['switch'], s['interfacelabel'])][s['direction']] = 1
        else:
            self.directions[(s['switch'], s['interfacelabel'])] = {s['direction']:1}

        self.streams[(s['switch'], s['interfacelabel'], s["direction"])] = s['stream_id']       
 

    def get_stream_id(self, params):
        if 'switch' not in params:
            return -1
        if 'interface' not in params:
            return -1
        if 'direction' not in params:
            return -1

        key = (params['switch'], params['interface'], params['direction'])
        if key not in self.streams:
            return -1
        
        return self.streams[key] 


    def get_aggregate_columns(self, detail):
        return ["bytes"]   
 
    def get_group_columns(self):
        return ["stream_id"]

    def format_data(self, received):
        return received

    def get_selection_options(self, params):
        
        # TODO - better handling of weird parameter combinations
        # e.g. what if they provide a interface but not a switch?

        if 'switch' not in params:
            return self._get_switches()

        if 'interface' not in params:
            return self._get_interfaces(params['switch'])

        if 'direction' not in params:
            return self._get_directions(params['switch'], params['interface'])

        # If we get here, they provided all the possible parameters so the
        # only available option is to return the matching stream (?)
        return [self.get_stream_id(params)]


    def _get_switches(self):
        """ Get the names of all switches that have munin data """
        return self.switches.keys()


    def _get_interfaces(self, switch):
        """ Get all available interfaces for a given switch """
        if switch != None:
            if switch not in self.interfaces:
                return []
            else:
                return self.interfaces[switch].keys()

        interfaces = {}
        for v in self.interfaces.values():
            for d in v.keys():
                interfaces[d] = 1
        return interfaces.keys()
 
    def _get_directions(self, switch, interface):
        """ Get all available directions for a given switch / interface combo """
        if switch != None and interface != None:
            if (switch, interface) not in self.directions:
                return []
            else:
                return self.directions[(switch, interface)].keys()

        dirs = {}
        for v in self.directions.values():
            for d in v.keys():
                dirs[d] = 1
        return dirs.keys()


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
