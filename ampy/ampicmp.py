#!/usr/bin/env python

import amp
import re

class AmpIcmpParser(amp.AmpParser):
    """ Parser for the amp-icmp collection. """

    def __init__(self, dbconfig):
        """ Initialises the parser """
        # we need all the same initialisation everything AMP related does
        super(AmpIcmpParser, self).__init__(dbconfig)

        # Dictionary that maps (source, dest) to a set of packet sizes that
        # have been used to test between those two hosts
        self.sizes = {}

        # Dictionary that maps (source, dest, size) to a set of addresses
        # that were used in tests between the two hosts
        self.addresses = {}

        self.splits = ["FULL", "NONE", "NETWORK", "FAMILY", "STREAM", "IPV4",
                "IPV6"]
        self.collection_name = "amp-icmp"

    # XXX do we want to extract the source/destination parts of this function
    # into the parent class?
    def add_stream(self, stream):
        """ Updates the internal maps based on a new stream

            Parameters:
                stream -- the new stream, as returned by NNTSC
        """
        super(AmpIcmpParser, self).add_stream(stream)
        src = stream['source']
        dest = stream['destination']
        sid = stream['stream_id']
        pktsize = stream['packet_size']

        if 'address' not in stream:
            address = "None"
        else:
            address = stream['address']

        if (src, dest) in self.sizes:
            self.sizes[(src, dest)][pktsize] = 1
        else:
            self.sizes[(src, dest)] = {pktsize:1}

        # Only do this if the stream has an address field
        key = (src, dest, pktsize)
        if key in self.addresses:
            self.addresses[key][address] = sid
        else:
            self.addresses[key] = {address:sid}

        if key in self.streams:
            self.streams[key].append(sid)
        else:
            self.streams[key] = [sid]

    def get_stream_id(self, params):
        """ Finds the stream IDs that matches the given parameters.

            If params does not contain an entry for 'source', 'destination',
            or 'packet_size', then [] will be returned.

            If params contains an entry for 'address', a list containing
            the unique stream id that matches all parameters will be returned.

            If 'address' is not provied, a list of stream ids covering all
            observed addresses for the source, dest, size combination will be
            returned.


            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                a list of stream ids for the matching stream(s), or an empty
                list if no matching streams can be found
        """

        if 'source' not in params:
            return []
        if 'destination' not in params:
            return []
        if 'packet_size' not in params:
            return []

        key = (params['source'], params['destination'], params['packet_size'])

        # If the address is explicitly provided, find the stream id that
        # belongs to that address specifically
        if 'address' in params:
            if key not in self.addresses:
                return []
            if params['address'] not in self.addresses[key]:
                return []
            return [self.addresses[key][params['address']]]

        # Otherwise, return all streams for the source, dest, size combo
        if key not in self.streams:
            return []
        return self.streams[key]

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """
        # the matrix view expects both the mean and stddev for the latency
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count", "avg", "count"]
            aggcols = ["rtt", "rtt", "rtt", "loss", "loss"]
        else:
            aggfuncs = ["avg", "avg"]
            aggcols = ["rtt", "loss"]

        # 'full' implies a smokeping-style graph, so we'll need to grab
        # the percentile data
        if detail == "full":
            ntiles = ["rtt"]
            others = ["loss"]
            ntileagg = ["avg"]
            otheragg = ["avg"]
            result = client.request_percentiles(colid, streams, start, end,
                    binsize, ntiles, others, ntileagg, otheragg)
        else:
            result = client.request_aggregate(colid, streams, start, end,
                    aggcols, binsize, ["stream_id"], aggfuncs)
        return result

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

        """
        # Don't do anything to this data, it is probably fine as is
        return received

    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            If '_requesting' is set to 'packet_sizes' and 'source' is set and
            'destination' is set, this will return the list of packet sizes
            used in tests between that source and that destination. If either
            of 'source' or 'destination' is not set, then a list of all
            packet sizes across all streams is returned.
        """
        # let the parent take care of this if it is an operation common to
        # all AMP tests: fetching sites, meshes, etc
        result = super(AmpIcmpParser, self).get_selection_options(params)
        if len(result) > 0:
            return result

        # otherwise check for the options specific to this type
        if params["_requesting"] == "packet_sizes":
            if 'source' not in params or 'destination' not in params:
                return []
            else:
                return self._get_sizes(params['source'], params['destination'])

        if params["_requesting"] == "addresses":
            if 'source' not in params or 'destination' not in params:
                return []
            if 'packet_size' not in params:
                sizekey = None
            else:
                sizekey = params['packet_size']
            return self._get_addresses(params['source'], params['destination'],
                    sizekey)

        return []

    def get_graphtab_stream(self, streaminfo, defaultsize=84):
        """ Given the description of a stream from a similar collection,
            return the stream id of the stream from this collection that is
            suitable for display on a graphtab alongside the main graph for
            the provided stream.
        """

        if 'source' not in streaminfo or 'destination' not in streaminfo:
            return []
        if 'address' not in streaminfo:
            return []

        sizes = self._get_sizes(streaminfo['source'],
                streaminfo['destination'])

        if sizes == []:
            return []

        params = {'source':streaminfo['source'],
                'destination':streaminfo['destination'],
                'address':streaminfo['address']}

        # First, try to find a packet size that matches the packet size of
        # the original stream.
        # If that fails, try to use a 84 byte packet size (as this is the
        # default for icmp tests)
        # If that fails, pick the smallest size available

        if 'packet_size' in streaminfo and streaminfo['packet_size'] in sizes:
            params['packet_size'] = streaminfo['packet_size']
        elif str(defaultsize) in sizes:
            params['packet_size'] = str(defaultsize)
        else:
            sizes.sort(key=int)
            params['packet_size'] = sizes[0]

        return [{'streamid':self.get_stream_id(params), 'title':"Latency", \
                'collection':'amp-icmp'}]

    def event_to_group(self, streaminfo):
        if '.' in streaminfo['address']:
            family = 'IPV4'
        else:
            family = 'IPV6'

        group = "%s FROM %s TO %s OPTION %s %s" % (
            self.collection_name, streaminfo["source"], 
            streaminfo["destination"],
            streaminfo["packet_size"], family)
        return group 
        
    
    def stream_to_group(self, streaminfo):
        group = "%s FROM %s TO %s OPTION %s STREAM %s" % (
            self.collection,_name, streaminfo["source"], 
            streaminfo["destination"],
            streaminfo["packet_size"], streaminfo['stream_id'])
        return group 
        

    def parse_group_options(self, options):
        if options[3].upper() not in self.splits:
            return None

        return "%s FROM %s TO %s OPTION %s %s" % (
                    self.collection_name, options[0], options[1], options[2],
                    options[3].upper())

    def split_group_rule(self, rule):
        parts = re.match("(?P<collection>[a-z-]+) "
                "FROM (?P<source>[.a-zA-Z0-9-]+) "
                "TO (?P<destination>[.a-zA-Z0-9-]+) "
                "OPTION (?P<option>[a-zA-Z0-9]+) "
                "(?P<split>[A-Z0-9]+)[ ]*(?P<stream>[0-9]*)", rule)
        if parts is None:
            return None, {}
        if parts.group("split") not in self.splits:
            return None, {}

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "packet_size": parts.group("option") 
        }


        return parts, keydict

    def legend_label(self, rule):
        parts, keydict = self.split_group_rule(rule)

        label = "%s to %s, %s bytes %s" % (parts.group('source'), 
                parts.group('destination'), parts.group('option'),
                parts.group('split'))
        if parts.group('split') == "STREAM":
            label += " %s" % (parts.group('stream'))

        return label

    def find_groups(self, parts, streams, groupid):
        collection = self.collection_name

        if parts.group("split") == "FULL":
            groups = self._get_combined_view_groups(collection,
                        parts, streams, groupid)
        elif parts.group("split") == "NONE":
            groups = self._get_all_view_groups(collection,
                        parts, streams, groupid)
        elif parts.group("split") == "NETWORK":
            groups = {}       # TODO
        elif parts.group("split") in ["FAMILY", "IPV4", "IPV6"]:
            groups = self._get_family_view_groups(collection,
                        parts, streams, groupid)
        elif parts.group("split") == "STREAM":
            groups = self._get_stream_view_groups(collection,
                        parts, streams, groupid)
        return groups


    def _get_combined_view_groups(self, collection, parts, streams, groupid):
        """ Combined all streams together into a single result line """
        key = "group_%s" % (groupid)
        return { key: {
                'streams':streams.keys(), 
                'source':parts.group('source'),
                'destination':parts.group('destination'),
                'packet_size':parts.group('option')
            }
        }
        
        
    def _get_all_view_groups(self, collection, parts, streams, groupid):
        """ Display all streams as individual result lines """
        groups = {} 
        for stream, info in streams.items():
            key = "group_%s_%s" % (groupid, info["address"])
            groups[key] = {
                    'streams':[stream],
                    'source':parts.group('source'),
                    'destination':parts.group('destination'),
                    'packet_size':parts.group('option')
            }
        return groups 
            
                
    def _get_family_view_groups(self, collection, parts, streams, groupid):
        """ Group streams by address family, displaying a line for ipv4/6 """
        groups = {} 
        for stream, info in streams.items():
            if "." in info["address"]:
                family = "ipv4"
            else:
                family = "ipv6"

            # If a specific family is requested, ignore all streams for the
            # other family
            if family == "ipv6" and parts.group("split") == "IPV4":
                continue
            if family == "ipv4" and parts.group("split") == "IPV6":
                continue

            key = "group_%s_%s" % (groupid, family)
            if key not in groups:
                groups[key] = {'streams':[],
                    'streams':[stream],
                    'source':parts.group('source'),
                    'destination':parts.group('destination'),
                    'packet_size':parts.group('option')
                }
            groups[key]['streams'].append(stream)
        return groups


    def _get_stream_view_groups(self, collection, parts, streams, groupid):
        """ Create a view containing a single stream """
        if int(parts.group("stream")) not in streams.keys():
            return {}
        key = "group_stream_%s" % (groupid, parts.group("stream"))
        return { key: {
                    'streams': [int(parts.group("stream"))],
                    'source':parts.group('source'),
                    'destination':parts.group('destination'),
                    'packet_size':parts.group('option')
                }
             }
 

    def _get_sizes(self, source, dest):
        """ Get a list of all packet sizes used to test between a given
            source and destination. If either source or dest is None, return
            all packet sizes that have been used across all streams.
        """
        if source != None and dest != None:
            if (source, dest) not in self.sizes:
                return []
            else:
                return self.sizes[(source, dest)].keys()
        sizes = {}
        for v in self.sizes.values():
            for d in v.keys():
                sizes[d] = 1
        return sizes.keys()

    def _get_addresses(self, source, dest, size):
        """ Get a list of all target addresses that have been observed for a
            given ICMP test.
        """
        if source == None or dest == None:
            return []

        if size == 0 or size == None:
            # No size specified, return all addresses for that source/dest
            # pair
            addrs = []
            for k, v in self.addresses.items():
                if k[0] == source and k[1] == dest:
                    addrs += v.keys()
            # Remove duplicates resulting from tests to the same address
            # using different packet sizes
            return list(set(addrs))
        if (source, dest, size) not in self.addresses:
            return []
        return self.addresses[(source, dest, size)].keys()


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
