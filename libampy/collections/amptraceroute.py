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
        if detail == "ippaths":
            aggfuncs = ["most", "most", "count", "most"]
            aggcols = ["error_type", "error_code", "path", "path_id"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["length"]
        
        return aggcols, aggfuncs
    
    def extra_blocks(self, detail):
        if detail == "full":
            return 2
        return 0

    def get_collection_history(self, cache, labels, start, end, detail,
            binsize):

        if detail != "ippaths":
            return super(AmpTraceroute, self).get_collection_history(cache,
                    labels, start, end, detail, binsize)

        uncached = {}
        paths = {}
        timeouts = []

        for lab in labels:
            cachelabel = lab['labelstring'] + "_ippaths_" + self.collection_name
            if len(cachelabel) > 128:
                log("Warning: ippath cache label %s is too long" % (cachelabel))
                
            cachehit = cache.search_ippaths(cachelabel, start, end)
            if cachehit is not None:
                paths[lab['labelstring']] = cachehit
                continue

            if len(lab['streams']) == 0:
                paths[lab['labelstring']] = []
            else:
                uncached[lab['labelstring']] = lab['streams']

        if len(uncached) > 0:
            result = self._fetch_history(uncached, start, end, end-start, 
                    detail)

            for label, queryresult in result.iteritems():
                if len(queryresult['timedout']) != 0:
                    timeouts.append(label)
                    paths[label] = []
                    continue 

                formatted = self.format_list_data(queryresult['data'], 
                        queryresult['freq'])

                cachelabel = lab['labelstring'] + "_ippaths_" + \
                        self.collection_name
                if len(cachelabel) > 128:
                    log("Warning: ippath cache label %s is too long" % \
                            (cachelabel))
                cache.store_ippaths(cachelabel, start, end, formatted)
                paths[label] = formatted


        return paths


class AmpAsTraceroute(AmpTraceroute):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpAsTraceroute, self).__init__(colid, viewmanager, nntscconf)
        self.collection_name = "amp-astraceroute"
        self.viewstyle = "amp-astraceroute"

    def group_columns(self, detail):
        return []
    
    def detail_columns(self, detail):
        if detail == "matrix":
            aggfuncs = ["avg"]
            aggcols = ["responses"]
        elif detail == "hops-full" or detail == "hops-summary":
            aggfuncs = ["most_array"]
            aggcols = ["aspath"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["responses"]
        
        return aggcols, aggfuncs
    
    def extra_blocks(self, detail):
        if detail == "hops-full" or detail == "full":
            return 2
        return 0


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
