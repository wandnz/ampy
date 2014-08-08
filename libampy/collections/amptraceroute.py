from libnntscclient.logger import *
from libampy.collection import Collection
from libampy.collections.ampicmp import AmpIcmp
import re
from operator import itemgetter

class AmpTraceroute(AmpIcmp):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpTraceroute, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'destination', 'packet_size',
                'family']
        self.groupproperties = ['source', 'destination', 'packet_size',
                'aggregation']
        self.collection_name = "amp-traceroute"
        self.default_packet_size = "60"
        self.viewstyle = "amp-traceroute"

    def group_columns(self, detail):
        if detail == "ippaths":
            return ['aspath', 'path']
        return []

    def detail_columns(self, detail):
        if detail == "matrix":
            aggfuncs = ["avg"]
            aggcols = ["length"]
        elif detail == "hops-full" or detail == "hops-summary":
            aggfuncs = ["most_array"]

            # TODO Replace with "aspath" when we start getting AS results
            #aggcols = ["aspath"]
            aggcols = ["path"]
        elif detail == "ippaths":
            aggfuncs = ["most", "most", "count"]
            aggcols = ["error_type", "error_code", "path"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["length"]
        
        return aggcols, aggfuncs
    
    def extra_blocks(self, detail):
        if detail == "hops-full" or detail == "full":
            return 2
        return 0


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
