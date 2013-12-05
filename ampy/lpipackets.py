#!/usr/bin/env python

import lpi

class LPIPacketsParser(lpi.LPIParser):

    def __init__(self):
        super(LPIPacketsParser, self).__init__()
        self.collection_name = "lpi-packets"
        self.tabtitle = "Packets"

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """
        aggcols = ["packets"]
        aggfuncs = ["avg"]
        group = ["stream_id"]
        return client.request_aggregate(colid, streams, start, end,
                aggcols, binsize, group, aggfuncs)



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
