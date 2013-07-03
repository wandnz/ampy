#!/usr/bin/env python

import sys, string

class MuninbytesParser(object):
    """ Parser for the rrd-muninbytes collection. """

    def __init__(self):
        """ Initialises the parser """

        # Maps (switch, interface, dir) to the corresponding stream id
        self.streams = {}

        # Maps (switch) to a set of interfaces on that switch
        self.interfaces = {}

        # Maps (switch, interface) to a set of valid directions on the interface
        self.directions = {}

        # Map containing the set of valid switches
        self.switches = {}

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream 

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

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
        """ Finds the stream ID that matches the given (switch, interface, dir)
            combination.

            If params does not contain an entry for 'switch', 'interface',
            or 'direction', then -1 will be returned.

            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                the id number of the matching stream, or -1 if no matching
                stream can be found
        """
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
        """ Return a list of columns in the data table for this collection
            that should be subject to data aggregation """
        
        # There is no "mininal" level of detail for this data, so just return
        # the same list regardless
        return ["bytes"]   
 
    def get_group_columns(self):
        """ Return a list of columns in the streams table that should be used
            to group aggregated data """
        return ["stream_id"]

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

            In the case of rrd-muninbytes, we need to convert the 'bytes per
            second' value stored in the database into Mbps
        """
        if "minres" not in streaminfo.keys():
            return received
        if streaminfo["minres"] == 0:
            return received

        for r in received:
            if "bytes" not in r.keys():
                continue
            if r["bytes"] == None:
                r["mbps"] = None
            else:
                # XXX bytes is an SNMP counter. What if we miss a measurement?
                r["mbps"] = ((float(r["bytes"]) * 8.0) / 1000000.0)

        return received

    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            If a 'switch' parameter is not given, this will return the list of 
            switches.

            If a 'switch' parameter is given but no 'interface' parameter, 
            this will return the list of interfaces on that switch.

            If a 'switch' and an 'interface' parameter are given but no
            'direction' parameter, this will return the list of directions for 
            that interface.

            If all three parameters are given, a list containing the ID of
            the stream described by those parameters is returned.
        """

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
