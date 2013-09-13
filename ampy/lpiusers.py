#!/usr/bin/env python

import sys, string

class LPIUsersParser(object):
    """ Parser for the lpi-metrics collection. """
    def __init__(self):
        """ Initialises the parser """
        self.streams = {}

        # Map containing all the valid sources
        self.sources = {}
        # Map containing all the valid protocols
        self.protocols = {}
        # Map containing all the valid metrics
        self.metrics = {}

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream 

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

        s['protocol'] = string.replace(s['protocol'], "/", " - ")

        self.sources[s['source']] = 1
        self.protocols[s['protocol']] = 1
        self.metrics[s['metric']] = 1

        self.streams[(s['source'], s['protocol'], s['metric'])] = s['stream_id']

    def get_stream_id(self, params):
        """ Finds the stream ID that matches the given (source, protocol,
            metric) combination.

            If params does not contain an entry for 'source', 'protocol'
            or 'metric', then -1 will be returned.

            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                the id number of the matching stream, or -1 if no matching
                stream can be found
        """

        if 'source' not in params:
            return -1;
        if 'protocol' not in params:
            return -1;
        if 'metric' not in params:
            return -1

        key = (params['source'], params['protocol'], params['metric'])
        if key not in self.streams:
            return -1
        return self.streams[key]

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """
        aggcols = ["users"]
        aggfuncs = ["avg"]
        group = ["stream_id"]

        return client.request_aggregate(colid, streams, start, end,
                aggcols, binsize, group, aggfuncs)

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

            In the case of lpi-users, no modification should be necessary.
        """

        return received


    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            params must have a field called "_requesting" which describes
            which of the possible stream parameters you are interested in.
        """

        if params['_requesting'] == 'sources':
            return self._get_sources()

        if params['_requesting'] == 'protocols':
            return self._get_protocols()
        
        if params['_requesting'] == 'metrics':
            return self._get_metrics()

        return []

    def get_graphtab_stream(self, streaminfo):
        """ Given the description of a streams from a similar collection,
            return the stream id of the streams from this collection that are
            suitable for display on a graphtab alongside the main graph (where
            the main graph shows the stream passed into this function)
        """

        if 'source' not in streaminfo or 'protocol' not in streaminfo:
            return []
    
        params = {'source':streaminfo['source'],
                'protocol':streaminfo['protocol'],
                'metric':'active'}
        active = self.get_stream_id(params)    
 
        params['metric'] = 'observed'
        observed = self.get_stream_id(params)

        ret = []
        if active != -1:
            ret.append({'streamid':active, 'title':'Users (Active)', 
                    'collection':'lpi-users'})
        if observed != -1:
            ret.append({'streamid':observed, 'title':'Users (Observed)', 
                    'collection':'lpi-users'})
        return ret

    def _get_sources(self):
        """ Get the names of all of the sources that have lpi flows data """
        return self.sources.keys()

    def _get_protocols(self):
        return self.protocols.keys()

    def _get_metrics(self):
        return self.metrics.keys()




# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
