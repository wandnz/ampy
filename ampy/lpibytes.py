#!/usr/bin/env python

import lpi

class LPIBytesParser(lpi.LPIParser):

    def __init__(self):
        super(LPIBytesParser, self).__init__()
        self.collection_name = "lpi-bytes"
        self.tabtitle = "Bytes"

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



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
