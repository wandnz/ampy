#!/usr/bin/env python

import sys, string, re

class LPIBytesParser(object):
    """ Parser for the lpi-bytes collection. """
    def __init__(self):
        """ Initialises the parser """
        self.streams = {}

        # Map containing all the valid sources
        self.sources = {}
        # Map containing all the valid protocols
        self.protocols = {}
        # Maps containing all the valid directions
        self.directions = {}
        # Maps (source, proto, dir) to a set of users
        self.users = {}

        self.groupsplits = ["IN", "OUT", "BOTH"]

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

        s['protocol'] = string.replace(s['protocol'], "/", "-")

        self.sources[s['source']] = 1
        self.protocols[s['protocol']] = 1
        
        if (s['source'], s['protocol']) in self.users:
            self.users[(s['source'], s['protocol'])][s['user']] = 1
        else:
            self.users[(s['source'], s['protocol'])] = {s['user']:1}
        

        key = (s['source'], s['protocol'], s['user'])

        if key in self.directions:
            self.directions[key][s['dir']] = s['stream_id']
        else:
            self.directions[key] = {s['dir']:s['stream_id']}

        if key in self.streams:
            self.streams[key].append(s['stream_id'])
        else:
            self.streams[key] = [s['stream_id']]


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

        key = (params['source'], params['protocol'], params['user'])
        
        if 'direction' in params:
            if key not in self.directions:
                return []
            if params['direction'] not in self.directions[key]:
                return []
            return [self.directions[key][params['direction']]]

        
        if key not in self.streams:
            return []
        return self.streams[key]

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """
        aggcols = ["bytes"]
        aggfuncs = ["avg"]
        group = ["stream_id"]
        return client.request_aggregate(colid, streams, start, end,
                aggcols, binsize, group, aggfuncs)

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

        if params['_requesting'] == 'users':
            if 'source' not in params or 'protocol' not in params:
                return self._get_users(None)
            return self._get_users(params)

        if params['_requesting'] == 'directions':
            if 'source' not in params or 'protocol' not in params or \
                    'user' not in params:
                return self._get_directions(None)
            return self._get_directions(params)

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
                'protocol':streaminfo['protocol']}

        # Hopefully direction will kinda go away as a parameter eventually.
        # Ideally, we would show 'in' and 'out' on the same graph
        if 'dir' not in streaminfo:
            params['direction'] = 'in'
        else:
            params['direction'] = streaminfo['dir']

        if 'user' not in streaminfo:
            params['user'] = 'all'
        else:
            params['user'] = streaminfo['user']

        return [{'streamid':self.get_stream_id(params), 'title':"Bytes", \
                'collection':'lpi-bytes'}]

    def event_to_group(self, streaminfo):
        group = "%s MONITOR %s PROTOCOL %s USER %s BOTH" % \
                ("lpi-bytes", streaminfo['source'], streaminfo['protocol'],
                streaminfo['user'])
        return group

    def stream_to_group(self, streaminfo):
        if streaminfo['dir'] == 'in':
            direction = "IN"
        elif streaminfo['dir'] == 'out':
            direction = "OUT"
        else:
            direction = "BOTH"
       
        print streaminfo 
        group = "%s MONITOR %s PROTOCOL %s USER %s %s" % \
                ("lpi-bytes", streaminfo['source'], streaminfo['protocol'],
                streaminfo['user'], direction)
        return group

    def parse_group_options(self, options):
        if options[4].upper() not in self.groupsplits:
            return None
        return "%s MONITOR %s PROTOCOL %s USER %s %s" % \
                (options[0], options[1], options[2], options[3], 
                options[4].upper())

    def split_group_rule(self, rule):
        parts = re.match("(?P<collection>[a-z-]+) "
                "MONITOR (?P<source>[.a-zA-Z0-9-]+) "
                "PROTOCOL (?P<protocol>\S+) "
                "USER (?P<user>\S+) "
                "(?P<direction>[A-Z]+)", rule)

        if parts is None:
            return None, {}
        if parts.group("direction") not in self.groupsplits:
            return None, {}

        keydict = {
            'source': parts.group('source'),
            'protocol': parts.group('protocol'),
            'user': parts.group('user')
        }

        return parts, keydict

    def find_groups(self, parts, streams, groupid):
        groups = {}
        partdir = parts.group('direction')

        for stream, info in streams.items():
            if info['dir'] == "in" and partdir == "OUT":
                continue
            if info['dir'] == "out" and partdir == "IN":
                continue

            key = "group_%s_%s" % (groupid, info['dir'])

            if key not in groups:
                groups[key] = {'streams':[]}
            groups[key]['streams'].append(stream)
            groups[key]['source'] = parts.group('source')
            groups[key]['protocol'] = parts.group('protocol')
            groups[key]['user'] = parts.group('user')
            groups[key]['direction'] = info['dir']

        return groups

    def legend_label(self, rule):
        parts, keydict = self.split_group_rule(rule)

        label = "%s %s for %s at %s %s" % (parts.group('protocol'), "bytes",
                parts.group('user'), parts.group('source'), 
                parts.group('direction'))
        return label
         
    def line_label(self, line):
        if line['direction'] == "in":
            return "Incoming"
        if line['direction'] == "out":
            return "Outgoing"
        return "Unknown" 


    def _get_sources(self):
        """ Get the names of all of the sources that have lpi bytes data """
        return self.sources.keys()

    def _get_users(self, params):
        """ Get all users that were measured by a given source """
        if params != None:
            key = (params['source'], params['protocol'])
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

    def _get_directions(self, params):
        if params == None:
            dirs = {}
            for v in self.directions.values():
                for d in v.keys():
                    dirs[d] = 1

            return dirs.keys()

        key = (params['source'], params['protocol'], params['user'])
        if key not in self.directions:
            return []
        else:
            return self.directions[key].keys()






# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
