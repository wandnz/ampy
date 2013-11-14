#!/usr/bin/env python

import sys, string
import amp

innercols = ['instance', 'query_type', 'query_class', 'udp_payload_size',
        'dnssec', 'recurse', 'nsid']

class AmpDnsParser(amp.AmpParser):

    def __init__(self, dbconfig):
        super(AmpDnsParser, self).__init__(dbconfig)

        self.queries = {}
        self.addresses = {}
        

    def add_stream(self, s):
        
        super(AmpDnsParser, self).add_stream(s)

        src = s['source']
        dest = s['destination']
        query = s['query']
        sid = s['stream_id']
        address = s['address']

        if (src, dest) in self.queries:
            self.queries[(src, dest)][query] = 1
        else:
            self.queries[(src, dest)] = {query:1}

        k = (src, dest, query)
        if k in self.addresses:
            self.addresses[k][address] = 1
        else:
            self.addresses[k] = {address:1}

        k = (src, dest, query, address)
        inner = {}

        for col in innercols:
            inner[col] = s[col]
        inner['stream_id'] = sid

        if k in self.streams:
            self.streams[k].append(inner)
        else:
            self.streams[k] = [inner]

    def get_stream_id(self, params):
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

    def request_data(self, client, colid, streams, start, end, binsize, 
            detail):
        
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count"]
            aggcols = ["rtt"]
        else:
            aggfuncs = ["avg"]
            aggcols = ["rtt"]

        if detail == "full":
            ntiles = ["rtt"]
            others = []
            ntileagg = ["avg"]
            otheragg = []
            result = client.request_percentiles(colid, streams, start,
                    end, binsize, ntiles, others, ntileagg, otheragg)
        else:
            result = client.request_aggregate(colid, streams, start,
                    end, aggcols, binsize, ["stream_id"], aggfuncs)

        return result

    def format_data(self, received, stream, streaminfo):
        return received


    def get_selection_options(self, params):
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

        for a in iteraddrs:
            k = (params['source'], params['destination'], query, a)
            if k not in self.streams:
                continue

            matchstreams = self.streams[k]

            for s in matchstreams:
                if req in s:            
                    possibles.append(s[req])

        return list(set(possibles))

    def get_graphtab_stream(self, streaminfo, defaultquery="www.google.com"):
        # This is probably only useful if we are running other AMP tests to
        # the DNS server
        
        if 'source' not in streaminfo or 'destination' not in streaminfo:
            return []
        if 'address' not in streaminfo:
            return []

        queries = self._get_queries(streaminfo['source'], 
                streaminfo['destination'])

        if queries == []:
            return []

        params = {'source': streaminfo['source'],
                'destination':streaminfo['destination'],
                'address':streaminfo['address']}

        if 'query' in streaminfo and streaminfo['query'] in queries:
            params['query'] = streaminfo['query']
        elif defaultquery in queries:
            params['query'] = defaultquery
        else:
            queries.sort()
            params['query'] = queries[0]

        return [{'streamid':self.get_stream_id(params), 'title':'DNS Latency', \
                'collection':'amp-dns'}]

    def _get_queries(self, source, dest):
        if (source, dest) not in self.queries:
            return []
        return self.queries[(source, dest)].keys()

    def _get_addresses(self, source, dest, query):
        if query == None:
            addrs = []
            for k,v in self.addresses.items():
                if k[0] == source and k[1] == dest:
                    addrs += v.keys()
            return list(set(addrs))

        if (source, dest, query) not in self.addresses:
            return []
        return self.addresses[(source, dest, query)].keys()

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :                
