#!/usr/bin/env python

import sys, string, re
import lpi

class LPIFlowsParser(lpi.LPIParser):
    """ Parser for the lpi-flows collection. """
    def __init__(self):
        super(LPIFlowsParser, self).__init__()
        self.collection_name = "lpi-flows"
        self.tabtitle = "Flows"

    def stream_to_key(self, s):
        if 'source' not in s:
            return None
        if 'protocol' not in s:
            return None
        if 'user' not in s:
            return None
        if 'metric' not in s:
            return None

        return (s['source'], s['protocol'], s['user'], s['metric'])
        

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """
        aggcols = ["flows"]
        aggfuncs = ["avg"]
        group = ["stream_id"]

        return client.request_aggregate(colid, streams, start, end,
                aggcols, binsize, group, aggfuncs)


    def get_graphtab_stream(self, streaminfo):
        """ Given the description of a stream from a similar collection,
            return the stream id of streams from this collection that are
            suitable for display on a graphtab alongside the main graph (where
            the main graph shows the stream that was passed into this function)
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

        params['metric'] = 'peak'
        peak = self.get_stream_id(params)

        params['metric'] = 'new'
        new = self.get_stream_id(params)

        ret = []
        if peak != -1:
            ret.append({'streamid':peak, 'title':'Flows (Peak)', \
                    'collection':'lpi-flows'})
        if new != -1:
            ret.append({'streamid':new, 'title':'Flows (New)', \
                    'collection':'lpi-flows'})

        return ret


    def event_to_group(self, streaminfo):
        group = "%s MONITOR %s PROTOCOL %s USER %s METRIC %s %s" % \
        (self.collection_name, streaminfo['source'], \
                streaminfo['protocol'],
                streaminfo['user'], streaminfo['metric'], direction)

        return group

    def stream_to_group(self, streaminfo):
        if streaminfo['dir'] == 'in':
            direction = "IN"
        elif streaminfo['dir'] == 'out':
            direction = "OUT"
        else:
            direction = "BOTH"

        group = "%s MONITOR %s PROTOCOL %s USER %s METRIC %s %s" % \
                (self.collection_name, streaminfo['source'], \
                streaminfo['protocol'],
                streaminfo['user'], streaminfo['metric'], direction)
        return group
        
    def parse_group_options(self, options):
        if options[5].upper() not in self.groupsplits:
            return None
        return "%s MONITOR %s PROTOCOL %s USER %s METRIC %s %s" % \
                (options[0], options[1], options[2], options[3], options[4],
                options[5].upper())

    def split_group_rule(self, rule):
        parts = re.match("(?P<collection>[a-z-]+) "
                "MONITOR (?P<source>[.a-zA-Z0-9-]+) "
                "PROTOCOL (?P<protocol>\S+) "
                "USER (?P<user>\S+) "
                "METRIC (?P<metric>[a-zA-Z0-9-]+) "
                "(?P<direction>[A-Z]+)", rule)

        if parts is None:
            return None, {}
        if parts.group("direction") not in self.groupsplits:
            return None, {}

        keydict = {
            'source': parts.group('source'),
            'protocol': parts.group('protocol'),
            'user': parts.group('user'),
            'metric': parts.group('metric')
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
            groups[key]['metric'] = parts.group('metric')
            groups[key]['direction'] = info['dir']

        return groups

    def legend_label(self, rule):
        parts, keydict = self.split_group_rule(rule)

        if parts.group('metric') == "new":
            metric = "new flows"
        elif parts.group('metric') == "peak":
            metric = "peak flows"
            
        label = "%s %s for %s at %s %s" % (parts.group('protocol'),
                metric,
                parts.group('user'), parts.group('source'),
                parts.group('direction'))
        return label
  


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
