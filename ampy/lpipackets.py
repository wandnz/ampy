#!/usr/bin/env python

import sys, string

class LPIPacketsParser(object):
    """ Parser for the lpi-packets collection. """
    def __init__(self):
        """ Initialises the parser """
        self.streams = {}

        # Map containing all the valid sources
        self.sources = {}
        # Maps (source, proto, dir) to a set of users measured from that source
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

        if (s['source'], s['protocol'], s['dir']) in self.users:
            self.users[(s['source'], s['protocol'], s['dir'])][s['user']] = 1
        else:
            self.users[(s['source'], s['protocol'], s['dir'])] = { s['user']:1 }
 
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

        return ['packets']

    def get_group_columns(self):
        """ Return a list of columns in the streams table that should be used
            to group aggregated data """

        return ['stream_id']

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

            In the case of lpi-packets, no modification should be necessary.
        """

        return received


    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            params must have a field called "_requesting" which describes
            which of the possible stream parameters you are interested in.

            If 'users' is requested, 'source' may also be set to receive only
            the list of users that are measured by that source. Otherwise,
            all users will be returned.
        """

        if params['_requesting'] == 'sources':
            return self._get_sources()

        if params['_requesting'] == 'protocols':
            return self._get_protocols()

        if params['_requesting'] == 'directions':
            return self._get_directions()

        if params['_requesting'] == 'users':
            if 'source' not in params or 'protocol' not in params or 'direction' not in params :
                return self._get_users(None)
            return self._get_users(params)


        return []

    def _get_sources(self):
        """ Get the names of all of the sources that have lpi packets data """
        return self.sources.keys()

    def _get_users(self, params):
        """ Get all users that were measured by a given source """

        if params != None:
            key = (params['source'], params['protocol'], params['direction'])
            if key not in self.users:
                return []
            else:
                return self.users[key].keys()

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
