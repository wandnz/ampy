#!/usr/bin/env python

import sys, string

class MuninbytesParser(object):
    """ Parser for the rrd-muninbytes collection. """

    def __init__(self):
        """ Initialises the parser """

        # Maps (switch, interface, dir) to the corresponding stream id
        self.streams = {}

        # Maps (switch) to a set of interfaces on that switch
        self.interfaces = {}

        # Maps (switch, interface) to a set of valid directions on the interface
        self.directions = {}

        # Map containing the set of valid switches
        self.switches = {}

        self.groupsplits = ["SENT", "RECEIVED", "BOTH"]

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

        self.switches[s['switch']] = 1

        key = (s['switch'], s['interfacelabel'])

        if s['switch'] in self.interfaces:
            self.interfaces[s['switch']][s['interfacelabel']] = 1
        else:
            self.interfaces[s['switch']] = {s['interfacelabel']:1}

        if key in self.directions:
            self.directions[key][s['direction']] = s['stream_id']
        else:
            self.directions[key] = {s['direction']:s['stream_id']}

        if key in self.streams:
            self.streams[key].append(s['stream_id'])
        else:
            self.streams[key] = [s['stream_id']]



    def get_stream_id(self, params):
        """ Finds the stream ID that matches the given (switch, interface, dir)
            combination.

            If params does not contain an entry for 'switch', 'interface',
            or 'direction', then -1 will be returned.

            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                the id number of the matching stream, or -1 if no matching
                stream can be found
        """
        if 'switch' not in params:
            return -1
        if 'interface' not in params:
            return -1

        key = (params['switch'], params['interface'])
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

            In the case of rrd-muninbytes, we need to convert the 'bytes per
            second' value stored in the database into Mbps
        """
        if "minres" not in streaminfo.keys():
            return received
        if streaminfo["minres"] == 0:
            return received

        for r in received:
            if "bytes" not in r.keys():
                continue
            if r["bytes"] == None:
                r["mbps"] = None
            else:
                # XXX bytes is an SNMP counter. What if we miss a measurement?
                r["mbps"] = ((float(r["bytes"]) * 8.0) / 1000000.0)

        return received

    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            If a 'switch' parameter is not given, this will return the list of
            switches.

            If a 'switch' parameter is given but no 'interface' parameter,
            this will return the list of interfaces on that switch.

            If a 'switch' and an 'interface' parameter are given but no
            'direction' parameter, this will return the list of directions for
            that interface.

            If all three parameters are given, a list containing the ID of
            the stream described by those parameters is returned.
        """

        # TODO - better handling of weird parameter combinations
        # e.g. what if they provide a interface but not a switch?
        if "_requesting" not in params:
            return []

        if params["_requesting"] == "switch":
            return self._get_switches()

        if params["_requesting"] == "interface":
            if "switch" not in params:
                return []
            return self._get_interfaces(params["switch"])

        if params["_requesting"] == "direction":
            if "switch" not in params or "interface" not in params:
                return []
            return self._get_directions(params['switch'], params['interface'])

        # If we get here, they provided all the possible parameters so the
        # only available option is to return the matching stream (?)
        return [self.get_stream_id(params)]

    def get_graphtab_group(self, parts, modifier):
        groupdict = parts
        if 'switch' not in groupdict or 'interface' not in groupdict:
            return None
        if 'direction' not in groupdict:
            direction = "BOTH"
        else:
            direction = groupdict['direction']

        group = "%s SWITCH-%s INTERFACE-%s %s" % (
                "rrd-muninbytes", groupdict['switch'], groupdict['interface'],
                direction)
        return group

    def event_to_group(self, streaminfo):
        group = "%s SWITCH-%s INTERFACE-%s BOTH" % (
                "rrd-muninbytes", streaminfo['switch'],
                streaminfo['interfacelabel'])
        return group

    def stream_to_group(self, streaminfo):
        if streaminfo['direction'] == 'sent':
            direction = "SENT"
        elif streaminfo['direction'] == 'received':
            direction = "RECEIVED"
        else:
            direction = "BOTH"

        group = "%s SWITCH-%s INTERFACE-%s %s" % (
                "rrd-muninbytes", streaminfo['switch'],
                streaminfo['interfacelabel'], direction)
        return group

    def parse_group_options(self, options):
        if len(options) != 3:
            return None
        if options[2].upper() not in self.groupsplits:
            return None

        return "%s SWITCH-%s INTERFACE-%s %s" % ("rrd-muninbytes",
                options[0], options[1], options[2].upper())

    def split_group_rule(self, rule):
        # Can't easily use regex here because SWITCH can be multiple
        # words :(
        parts = {}

        switchind = rule.find(" SWITCH-")
        interind = rule.rfind(" INTERFACE-")
        dirind = rule.rfind(" ")

        assert(interind >= switchind + len(" SWITCH-"))

        parts['switch'] = rule[switchind + len(" SWITCH-"):interind]
        parts['collection'] = rule[0:switchind]
        parts['interface'] = rule[interind + len(" INTERFACE-"):dirind]
        parts['direction'] = rule[dirind + 1:]

        if parts["direction"] not in self.groupsplits:
            return None, {}

        keydict = {
            'switch':parts['switch'],
            'interface':parts['interface']
        }

        return parts, keydict

    def find_groups(self, parts, streams, groupid):
        groups = {}
        partdir = parts['direction']

        for stream, info in streams.items():
            if info['direction'] == "sent" and partdir == "RECEIVED":
                continue
            if info['direction'] == "received" and partdir == "SENT":
                continue

            key = "group_%s_%s" % (groupid, info['direction'])

            if key not in groups:
                groups[key] = {'streams':[]}
            groups[key]['streams'].append(stream)
            groups[key]['switch'] = parts['switch']
            groups[key]['interface'] = parts['interface']
            groups[key]['direction'] = info['direction']

        return groups

    def legend_label(self, rule):
        parts, keydict = self.split_group_rule(rule)

        label = "%s: %s %s" % (parts['switch'], parts['interface'],
                parts['direction'])
        return label

    def line_label(self, line):
        # Keep these short

        if line['direction'] == "received":
            return "Received"
        if line['direction'] == "sent":
            return "Sent"
        return "Unknown"

    def _get_switches(self):
        """ Get the names of all switches that have munin data """
        return self.switches.keys()


    def _get_interfaces(self, switch):
        """ Get all available interfaces for a given switch """
        if switch != None:
            if switch not in self.interfaces:
                return []
            else:
                return self.interfaces[switch].keys()

        interfaces = {}
        for v in self.interfaces.values():
            for d in v.keys():
                interfaces[d] = 1
        return interfaces.keys()

    def _get_directions(self, switch, interface):
        """ Get all available directions for a given switch / interface combo """
        if switch != None and interface != None:
            if (switch, interface) not in self.directions:
                return []
            else:
                return self.directions[(switch, interface)].keys()

        dirs = {}
        for v in self.directions.values():
            for d in v.keys():
                dirs[d] = 1
        return dirs.keys()


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
