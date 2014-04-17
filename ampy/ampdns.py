#!/usr/bin/env python

import amp
import re

innercols = ['instance', 'query_type', 'query_class', 'udp_payload_size',
        'dnssec', 'recurse', 'nsid']

class AmpDnsParser(amp.AmpParser):

    def __init__(self, dbconfig):
        super(AmpDnsParser, self).__init__(dbconfig)

        self.queries = {}
        self.addresses = {}

        self.splits = ["NONE", "FULL"]
        self.collection_name = "amp-dns"

    def _split_to_human(self, split, instance):
        if split == "FULL":
            return ""
        if split == "NONE":
            return "per instance"

        return ""

    def add_stream(self, stream):

        super(AmpDnsParser, self).add_stream(stream)

        src = stream['source']
        dest = stream['destination']
        query = stream['query']
        sid = stream['stream_id']
        address = stream['address']

        if (src, dest) in self.queries:
            self.queries[(src, dest)][query] = 1
        else:
            self.queries[(src, dest)] = {query:1}

        key = (src, dest, query)
        if key in self.addresses:
            self.addresses[key][address] = 1
        else:
            self.addresses[key] = {address:1}

        key = (src, dest, query, address)
        inner = {}

        for col in innercols:
            inner[col] = stream[col]
        inner['stream_id'] = sid

        if key in self.streams:
            self.streams[key].append(inner)
        else:
            self.streams[key] = [inner]


    def get_stream_id(self, params):
        """ Get stream IDs that match the given test parameters """

        if 'source' not in params:
            return []
        if 'destination' not in params:
            return []
        if 'query' not in params:
            return []

        src = params['source']
        dest = params['destination']
        query = params['query']
        # Remove these to reduce the iteration workload later on
        del params['source']
        del params['destination']
        del params['query']

        # If address isn't specified, return streams for all valid
        # addresses
        if 'address' in params:
            addresses = [params['address']]
            del params['address']
        else:
            k = (src, dest, query)

            if k not in self.addresses:
                return []
            addresses = self.addresses[k].keys()

        # Iterate through all the valid src, dest, query, address combinations,
        # ruling out streams that do match any additional parameters specified.

        # For example, if params has a key 'query_type' with a value 'A' we
        # don't want to return streams that don't have a query_type of 'A'.

        # XXX This must be hideously slow, but hopefully we'll soon have a
        # views system that renders this sort of thing unnecessary (or at
        # least does it better)

        result = []
        for a in addresses:
            k = (src, dest, query, a)

            # First, check that this is a valid combination
            if k not in self.streams:
                continue

            # This describes all possible streams for that combo
            matchstreams = self.streams[k]

            # Otherwise, discard any streams which do not match the specific
            # parameters passed into this function
            for s in matchstreams:
                discard = False
                for p, val in params.items():
                    if p in s and s[p] != val:
                        discard = True
                        break
                if not discard:
                    result.append(s['stream_id'])

        return result

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Query NNTSC for data, aggregated appropriately based on detail """

        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count"]
            aggcols = ["rtt"]
        elif detail == "full":
            aggfuncs = ["smoke"]
            aggcols = ["rtt"]
        else:
            aggfuncs = ["avg"]
            aggcols = ["rtt"]

        result = client.request_aggregate(colid, streams, start,
            end, aggcols, binsize, ["stream_id"], aggfuncs)

        return result


    def format_data(self, received, stream, streaminfo):
        """ Format the data if any other changes required, not in this case """
        return received


    def get_selection_options(self, params):
        """ Get possible values for a selection item """
        result = super(AmpDnsParser, self).get_selection_options(params)
        if len(result) > 0:
            return result

        if 'source' not in params or 'destination' not in params:
            return []

        if 'query' not in params:
            query = None
        else:
            query = params['query']

        if 'address' not in params:
            address = None
        else:
            address = params['address']

        if params["_requesting"] == "queries":
            return self._get_queries(params['source'], params['destination'])

        if params["_requesting"] == "addresses":
            return self._get_addresses(params['source'], params['destination'],
                    query)

        if params['_requesting'] not in innercols:
            return []

        if query == None:
            return []
        if address == None:
            iteraddrs = self._get_addresses(params['source'],
                    params['destination'], query)
        else:
            iteraddrs = [address]

        possibles = []
        req = params['_requesting']

        # XXX A lot of loops here, although hopefully most of these are
        # only a handful of items max (if not one)
        for a in iteraddrs:
            key = (params['source'], params['destination'], query, a)
            if key not in self.streams:
                continue

            matchstreams = self.streams[key]

            for stream in matchstreams:
                ignore = False

                # Ignore any streams that don't match any additional
                # parameters provided
                for key, val in stream.iteritems():
                    if key in params and params[key] != val:
                        ignore = True
                        break

                if not ignore and req in stream:
                    possibles.append(stream[req])

        return list(set(possibles))


    def _get_queries(self, source, dest):
        if (source, dest) not in self.queries:
            return []
        return self.queries[(source, dest)].keys()


    def get_graphtab_group(self, parts):

        groupdict = parts.groupdict()
        if 'source' not in groupdict or 'destination' not in groupdict:
            return None

        queries = self._get_queries(groupdict['source'],
                groupdict['destination'])

        if 'query' not in groupdict:
            query = "www.google.com"
        else:
            query = groupdict['query']

        if queries != []:
            if query not in queries:
                if "www.google.com" in queries:
                    query = "www.google.com"
                else:
                    queries.sort()
                    query = queries[0]

        if 'query_class' not in groupdict:
            queryclass = "IN"
        else:
            queryclass = groupdict['query_class']

        if 'query_type' not in groupdict:
            qtype = "A"
        else:
            qtype = groupdict['query_type']

        if 'udp_payload_size' not in groupdict:
            psize = "4096"
        else:
            psize = groupdict['udp_payload_size']

        if 'flags' not in groupdict:
            flags = "TFF"
        else:
            flags = groupdict['flags']

        if 'split' not in groupdict:
            split = "FULL"
        else:
            split = groupdict['split']

        group = "%s FROM %s TO %s OPTION %s %s %s %s %s %s" % (
            self.collection_name, groupdict['source'],
            groupdict['destination'], query, queryclass, qtype, psize,
            flags, split)

        return group

    def _get_flags_string(self, streaminfo):
        flags = ""
        if streaminfo['recurse'] == True:
            flags += "T"
        else:
            flags += "F"

        if streaminfo['dnssec'] == True:
            flags += "T"
        else:
            flags += "F"

        if streaminfo['nsid'] == True:
            flags += "T"
        else:
            flags += "F"
        return flags

    def event_to_group(self, streaminfo):
        flags = self._get_flags_string(streaminfo)

        # the event graph should merge all the instances together into one
        group = "%s FROM %s TO %s OPTION %s %s %s %s %s FULL" % (
            self.collection_name, streaminfo["source"],
            streaminfo["destination"], streaminfo["query"],
            streaminfo["query_class"], streaminfo["query_type"],
            streaminfo["udp_payload_size"], flags)
        return group


    def _get_addresses(self, source, dest, query):
        if query == None:
            addrs = []
            for k, v in self.addresses.items():
                if k[0] == source and k[1] == dest:
                    addrs += v.keys()
            return list(set(addrs))

        if (source, dest, query) not in self.addresses:
            return []
        return self.addresses[(source, dest, query)].keys()


    def stream_to_group(self, streaminfo):
        """ Convert a stream to a group description string """
        flags = self._get_flags_string(streaminfo)
        group = "%s FROM %s TO %s OPTION %s %s %s %s %s INSTANCE %s" % (
            self.collection_name, streaminfo["source"],
            streaminfo["destination"], streaminfo["query"],
            streaminfo["query_class"], streaminfo["query_type"],
            streaminfo["udp_payload_size"], flags,
            streaminfo['instance'])
        return group


    def parse_group_options(self, options):
        if len(options) != 8:
            return None
        """ Convert group options array into a group description string """
        return "%s FROM %s TO %s OPTION %s %s %s %s %s %s" % (
                    self.collection_name, options[0], options[1], options[2],
                    options[3], options[4], options[5], options[6], options[7])


    def split_group_rule(self, rule):
        """ Split a group description string into test option parts """

        parts = re.match("(?P<collection>[a-z-]+) "
                "FROM (?P<source>[.a-zA-Z0-9-]+) "
                "TO (?P<destination>[.a-zA-Z0-9-:]+) "
                "OPTION (?P<query>[a-zA-Z0-9.]+) IN (?P<type>[A-Z]+) "
                "(?P<size>[0-9]+) (?P<flags>[TF]+) "
                "(?P<split>[A-Z]+)[ ]*(?P<instance>[.a-zA-Z0-9-:]*)", rule)
        if parts is None:
            return None, None
        if parts.group("split") not in self.splits:
            return None, None

        if len(parts.group("flags")) != 3:
            return None, None

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "query": parts.group("query"),
            "query_class": "IN",
            "query_type": parts.group("type"),
            "udp_payload_size": int(parts.group("size")),
            "recurse": parts.group("flags")[0] == "T",
            "dnssec": parts.group("flags")[1] == "T",
            "nsid": parts.group("flags")[2] == "T",
        }

        return parts, keydict


    def legend_label(self, rule):
        parts, keydict = self.split_group_rule(rule)
        if parts is None:
            print "Failed to parse DNS group rule:", rule
            return ""

        flags = ""
        if keydict["recurse"]:
            flags += "+recurse "
        if keydict["dnssec"]:
            flags += "+dnssec "
        if keydict["nsid"]:
            flags += "+nsid "

        flags = flags.strip()
        label = "%s to %s, %s %s %s %s %s %s" % (keydict["source"],
                keydict["destination"], keydict["query"],
                keydict["query_class"], keydict["query_type"],
                keydict["udp_payload_size"], flags,
                self._split_to_human(parts.group('split'),
                        parts.group('instance')))
        return label


    def line_label(self, line):
        if 'shortlabel' in line:
            return line['shortlabel']
        return 'Unknown'


    def find_groups(self, parts, streams, groupid):
        """ Split a list of streams into groups based on the value in parts """
        collection = self.collection_name

        if parts.group("split") == "NONE":
            groups = self._get_all_view_groups(collection,
                        parts, streams, groupid)
        elif parts.group("split") == "FULL":
            groups = self._get_combined_view_groups(collection,
                        parts, streams, groupid)
        return groups


    def _get_all_view_groups(self, collection, parts, streams, groupid):
        """ Display all streams/instances as individual result lines """
        groups = {}
        for stream, info in streams.items():
            key = "group_%s_%s" % (groupid, info["address"])
            groups[key] = {
                    "streams": [stream],
                    "source": parts.group("source"),
                    "destination": parts.group("destination"),
                    "query": parts.group("query"),
                    "query_class": "IN",
                    "query_type": parts.group("type"),
                    "udp_payload_size": parts.group("size"),
                    "recurse": info['recurse'],
                    "dnssec": info['dnssec'],
                    "nsid": info['nsid'],
                    "shortlabel": "%s (%s)" % (info["instance"],info["address"])
            }
        return groups


    def _get_combined_view_groups(self, collection, parts, streams, groupid):
        """ Combined all streams together into a single result line """
        key = "group_%s" % (groupid)
        return { key: {
                "streams": streams.keys(),
                "source": parts.group("source"),
                "destination": parts.group("destination"),
                "query": parts.group("query"),
                "query_class": "IN",
                "query_type": parts.group("type"),
                "udp_payload_size": parts.group("size"),
                "recurse": parts.group("flags")[0],
                "dnssec": parts.group("flags")[1],
                "nsid": parts.group("flags")[2],
                "shortlabel": "All instances"
            }
        }

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
