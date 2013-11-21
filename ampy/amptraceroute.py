#!/usr/bin/env python

import sys, string
import ampicmp

class AmpTracerouteParser(ampicmp.AmpIcmpParser):
    """ Parser for the amp-traceroute collection. """

    def __init__(self, dbconfig):
        """ Initialises the parser """
        # we need all the same initialisation everything AMP related does
        super(AmpTracerouteParser, self).__init__(dbconfig)

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """

        if detail == "matrix":
            aggcols = ["length", "length"]
            aggfuncs = ["avg", "count"]
            group = ["stream_id"]

            # XXX this returns average path lengths, but will this add each
            # length n times and take the average of that? there are more rows
            # the longer the path...
            return client.request_aggregate(colid, streams, start, end, aggcols,
                    binsize, group, aggfuncs)
        else:
            # TODO half of these probably won't work? also missing hop latency
            commoncols = ["hop_address"]
            othercols = ["length", "error_type", "error_code"]
            otheragg = ["most"]
            return client.request_common(colid, streams, start, end, binsize,
                    commoncols, othercols, otheragg)

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

        """
        # Don't do anything to this data, it is probably fine as is
        return received

    def get_graphtab_stream(self, streaminfo):
        """ Given the description of a stream from a similar collection,
            return the stream id of the stream from this collection that is
            suitable for display on a graphtab alongside the main graph for
            the provided stream.
        """
        result = super(AmpTracerouteParser, self).get_graphtab_stream(streaminfo, "60")

        # Our parent class is going set these to be amp-icmp, so replace them
        # with something more suitable for traceroute

        if result != []:
            result[0]['title'] = "Traceroute"
            result[0]["collection"] = "amp-traceroute"
        return result

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
