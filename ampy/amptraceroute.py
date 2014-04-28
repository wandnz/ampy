#!/usr/bin/env python

import ampicmp

class AmpTracerouteParser(ampicmp.AmpIcmpParser):
    """ Parser for the amp-traceroute collection. """

    def __init__(self, dbconfig):
        """ Initialises the parser """
        # we need all the same initialisation everything AMP related does
        super(AmpTracerouteParser, self).__init__(dbconfig)

        self.collection_name = "amp-traceroute"

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """

        if detail == "matrix":
            # matrix display is only interested in path lengths
            aggfuncs = ["avg"]
            aggcols = ["length"]
        elif detail == "hops":
            # other displays are interested in the actual path
            aggfuncs = ["most_array"]
            aggcols = ["path"] # TODO get hop_rtt as well
        else:
            # Display path length using smokeping-style graph
            aggfuncs = ["smoke"]
            aggcols = ["length"]

        return client.request_aggregate(colid, streams, start, end,
            aggcols, binsize, ["stream_id"], aggfuncs)


    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

        """
        # Don't do anything to this data, it is probably fine as is
        return received

    def get_graphtab_group(self, parts):
        return super(AmpTracerouteParser, self).get_graphtab_group(parts,"60")

    def event_to_group(self, streaminfo):
        group = "%s FROM %s TO %s OPTION %s ADDRESS %s" % (
                self.collection_name, streaminfo["source"],
                streaminfo["destination"], streaminfo["packet_size"],
                streaminfo["address"])

        return group

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
