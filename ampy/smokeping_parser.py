#!/usr/bin/env python

import sys, string

class SmokepingParser(object):

    def __init__(self):
        self.streams = {}
        self.sources = {}
        self.destinations = {}

    def add_stream(self, s):
        if s['host'] in self.sources:
            self.sources[s['host']][s['source']] = 1
        else:
            self.sources[s['host']] = {s['source']:1}

        if s['source'] in self.destinations:
            self.destinations[s['source']][s['host']] = 1
        else:
            self.destinations[s['source']] = {s['host']:1}

        self.streams[(s['source'], s['host'])] = s['stream_id']

    def get_stream_id(self, params):
        if 'source' not in params:
            return -1
        if 'host' not in params:
            return -1
        
        key = (params['source'], params['host'])
        if key not in self.streams:
            return -1
        return self.streams[key]
        
    def get_aggregate_columns(self, detail):

        if detail == "minimal":
            return ["median", "loss"]

        return ['uptime', 'loss', 'median',
            'ping1', 'ping2', 'ping3', 'ping4', 'ping5', 'ping6', 'ping7', 
            'ping8', 'ping9', 'ping10', 'ping11', 'ping12', 'ping13', 
            'ping14', 'ping15', 'ping16', 'ping17', 'ping18', 'ping19', 
            'ping20']

    def get_group_columns(self):
        return ["stream_id"]

    def format_data(self, received):
        formatted = []

        for d in received:
            newdict = {}
            pings = [None] * 20
            export_pings = False
            for k, v in d.items():

                if "ping" in k:
                    index = int(k.split("ping")[1]) - 1
                    assert(index >= 0 and index < 20)
                    pings[index] = v
                    export_pings = True
                else:
                    newdict[k] = v

            if export_pings: 
                newdict["pings"] = pings

            formatted.append(newdict)
        return formatted

    def get_selection_options(self, params):
        if 'source' not in params and 'host' not in params:
            return self._get_sources(None)

        if 'source' not in params:
            return self._get_sources(params['host'])
  
        if 'host' not in params:
            return self._get_destinations(params['source'])

        return [self.get_stream_id(params)]

    def _get_sources(self, dst):
        if dst != None:
            if dst not in self.sources:
                return []
            return self.sources[dst].keys()

        sources = {}
        for v in self.sources.values():
            for src in v.keys():
                sources[src] = 1
        return sources.keys()

    def _get_destinations(self, src):
        if src != None:
            if src not in self.destinations:
                return []
            return self.destinations[src].keys()

        dests = {}
        for v in self.destinations.values():
            for d in v.keys():
                dests[d] = 1
        return dests.keys()



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
