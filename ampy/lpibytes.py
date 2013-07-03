#!/usr/bin/env python

import sys, string

class LPIBytesParser(object):
    """ Parser for the lpi-bytes collection. """
    def __init__(self):
        """ Initialises the parser """
        self.streams = {}

        # Map containing all the valid sources
        self.sources = {}
        # Maps (source) to a set of users measured from that source
        self.users = {}
        # Map containing all the valid protocols
        self.protocols = {}
        # Maps containing all the valid directions
        self.directions = {}

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream 

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

        s['protocol'] = string.replace(s['protocol'], "/", " - ")

        self.sources[s['source']] = 1
        self.protocols[s['protocol']] = 1
        self.directions[s['dir']] = 1

        if s['source'] in self.users:
            self.users[s['source']][s['user']] = 1
        else:
            self.users[s['source']] = { s['user']:1 }

        
        self.streams[(s['source'], s['user'], s['protocol'], s['dir'])] = s['stream_id']

    def get_stream_id(self, params):
        """ Finds the stream ID that matches the given (source, user, protocol,
            direction) combination.

            If params does not contain an entry for 'source', 'user',
            'protocol' or 'direction', then -1 will be returned.

            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                the id number of the matching stream, or -1 if no matching
                stream can be found
        """

        if 'source' not in params:
            return -1;
        if 'user' not in params:
            return -1;
        if 'protocol' not in params:
            return -1;
        if 'direction' not in params:
            return -1;

        key = (params['source'], params['user'], params['protocol'], params['direction'])
        if key not in self.streams:
            return -1
        return self.streams[key]

    def get_aggregate_columns(self, detail):
        """ Return a list of columns in the data table for this collection
            that should be subject to data aggregation """

        return ['bytes']

    def get_group_columns(self):
        """ Return a list of columns in the streams table that should be used
            to group aggregated data """

        return ['stream_id']

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

            In the case of lpi-bytes, we need to convert the 'bytes' value 
            stored in the database into Mbps
        """

        if 'freq' not in streaminfo.keys():
            return received
        if streaminfo['freq'] == 0:
            return received

        for r in received:
            if 'bytes' not in r.keys():
                continue
            if r['bytes'] == None:
                r['mbps'] = None
            else:
                r['mbps'] = ((float(r['bytes']) * 8.0) / streaminfo['freq'] / 1000000.0)
        return received


    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            If a 'source' parameter is not given, this will return the list of 
            sources.

            If a 'source' parameter is given but no 'user' parameter, 
            this will return the list of users measured by that source.

            If a 'source' and an 'user' parameter are given but no
            'protocol' parameter, this will return the list of protocols for 
            that source.

            If a 'source', a 'user' and a 'protocol' parameter are given but
            no 'direction' parameter, this will return the list of possible
            directions.

            If all four parameters are given, a list containing the ID of
            the stream described by those parameters is returned.
        """

        if params['_requesting'] == 'sources':
            return self._get_sources()

        if params['_requesting'] == 'protocols':
            return self._get_protocols()

        if params['_requesting'] == 'directions':
            return self._get_directions()

        if params['_requesting'] == 'users':
            if 'source' in params:
                return self._get_users(params['source'])
            else:
                return self._get_users(None)

        return [self.get_stream_id(params)]

    def _get_sources(self):
        """ Get the names of all of the sources that have lpi bytes data """
        return self.sources.keys()

    def _get_users(self, source):
        """ Get all users that were measured by a given source """
        if source != None:
            if source not in self.users:
                return []
            else:
                return self.users[source].keys()

        users = {}
        for v in self.users.values():
            for d in v.keys():
                users[d] = 1
        return users.keys()


    def _get_protocols(self):
        return self.protocols.keys()

    def _get_directions(self):
        return self.directions.keys()




# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
