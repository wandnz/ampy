#!/usr/bin/env python

import sys, string, re

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

        self.groupsplits = ["OBSERVED", "ACTIVE", "BOTH"]
        self.collection_name = "lpi-users"

    def stream_to_key(self, s):
        if 'source' not in s:
            return None
        if 'protocol' not in s:
            return None
        return (s['source'], s['protocol'])

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

        s['protocol'] = string.replace(s['protocol'], "/", "-")

        self.sources[s['source']] = 1
        self.protocols[s['protocol']] = 1

        key = self.stream_to_key(s)
        assert(key is not None)

        if key in self.metrics:
            self.metrics[key][s['metric']] = s['stream_id']
        else:
            self.metrics[key] = {s['metric']:s['stream_id']}

        if key in self.streams:
            self.streams[key].append(s['stream_id'])
        else:
            self.streams[key] = [s['stream_id']]

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

        key = self.stream_to_key(params)
        if key is None:
            return []

        if 'metric' in params:
            if key not in self.metrics:
                return []
            if params['metric'] not in self.metrics[key]:
                return []
            return [self.metrics[key][params['metric']]]

        if key not in self.streams:
            return []
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
        if "_requesting" not in params:
            return []

        if params['_requesting'] == 'source':
            return self._get_sources()

        if params['_requesting'] == 'protocol':
            return self._get_protocols()

        if params['_requesting'] == 'metric':
            if 'source' not in params or 'protocol' not in params:
                return self._get_metrics(None)
            return self._get_metrics(params)

        return []

    def get_graphtab_group(self, parts, modifier):

        groupdict = parts.groupdict()
        if 'source' not in groupdict or 'protocol' not in groupdict:
            return None

        if 'metric' not in groupdict or \
                groupdict['metric'] not in self.groupsplits:
            metric = "BOTH"
        else:
            metric  = groupdict['metric']

        group = "%s MONITOR %s PROTOCOL %s %s" % \
                (self.collection_name, groupdict['source'], 
                groupdict['protocol'], metric)
        return group
 

    def event_to_group(self, streaminfo):
        group = "%s MONITOR %s PROTOCOL %s BOTH" % \
                ("lpi-users", streaminfo['source'], streaminfo['protocol'])
        return group

    def stream_to_group(self, streaminfo):
        if streaminfo['metric'] == 'observed':
            metric = "OBSERVED"
        elif streaminfo['metric'] == 'active':
            metric = "ACTIVE"
        else:
            metric = "BOTH"

        group = "%s MONITOR %s PROTOCOL %s %s" % \
                ("lpi-users", streaminfo['source'], streaminfo['protocol'],
                metric)
        return group


    def parse_group_options(self, options):
        if len(options) != 3:
            return None
        if options[2].upper() not in self.groupsplits:
            return None
        return "%s MONITOR %s PROTOCOL %s %s" % (
                self.collection_name, options[0], options[1],
                options[2].upper())

    def split_group_rule(self, rule):
        parts = re.match("(?P<collection>[a-z-]+) "
                "MONITOR (?P<source>[.a-zA-Z0-9-]+) "
                "PROTOCOL (?P<protocol>\S+) "
                "(?P<metric>[A-Z]+)", rule)
        if parts is None:
            return None, {}
        if parts.group("metric") not in self.groupsplits:
            return None, {}

        keydict = {
            'source': parts.group('source'),
            'protocol': parts.group('protocol')
        }

        return parts, keydict

    def find_groups(self, parts, streams, groupid):
        groups = {}
        partmet = parts.group('metric')

        for stream, info in streams.items():
            if info['metric'] == 'observed' and partmet == "ACTIVE":
                continue
            if info['metric'] == 'active' and partmet == "OBSERVED":
                continue

            key = "group_%s_%s" % (groupid, info['metric'])
            if key not in groups:
                groups[key] = {'streams':[]}
            groups[key]['streams'].append(stream)
            groups[key]['source'] = parts.group('source')
            groups[key]['protocol'] = parts.group('protocol')
            groups[key]['metric'] = info['metric']
        return groups


    def legend_label(self, rule):
        parts, keydict = self.split_group_rule(rule)

        label = "%s users at %s (%s)" % (parts.group('protocol'),
                parts.group('source'), parts.group('metric'))
        return label

    def line_label(self, line):
        if line['metric'] == 'active':
            return "Active"
        if line['metric'] == 'observed':
            return "Observed"
        return "Unknown"

    def _get_sources(self):
        """ Get the names of all of the sources that have lpi flows data """
        return self.sources.keys()

    def _get_protocols(self):
        return self.protocols.keys()

    def _get_metrics(self, params):

        if params != None:
            key = (params['source'], params['protocol'])
            if key not in self.metrics:
                return []
            return self.metrics[key].keys()

        metrics = {}
        for v in self.metrics.values():
            for d in v.keys():
                metrics[d] = 1
        return metrics.keys()




# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
