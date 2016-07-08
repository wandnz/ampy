from libnntscclient.logger import *
from libampy.collection import Collection
from libampy.collections.ampicmp import AmpIcmp
import re, socket
from operator import itemgetter

class AmpTraceroute(AmpIcmp):
    def __init__(self, colid, viewmanager, nntscconf, asnmanager):
        super(AmpTraceroute, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'destination', 'packet_size',
                'family']
        self.groupproperties = ['source', 'destination', 'packet_size',
                'aggregation']
        self.collection_name = "amp-traceroute"
        self.default_packet_size = "60"
        self.default_aggregation = "FAMILY"
        self.viewstyle = "amp-traceroute"

        self.asnmanager = asnmanager
    def group_columns(self, detail):
        if detail == "ippaths":
            return ['aspath', 'path']
        return []

    def detail_columns(self, detail):
        if detail == "ippaths":
            aggfuncs = ["most", "most", "count", "most"]
            aggcols = ["error_type", "error_code", "path", "path_id"]
        elif detail == "ippaths-summary":
            aggfuncs = ["count"]
            aggcols = ["path_id"]
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

        # Save the cache because we'll want it for our AS name lookups    
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
                        queryresult['freq'], detail)

                cachelabel = lab['labelstring'] + "_ippaths_" + \
                        self.collection_name
                if len(cachelabel) > 128:
                    log("Warning: ippath cache label %s is too long" % \
                            (cachelabel))
                cache.store_ippaths(cachelabel, start, end, formatted)
                paths[label] = formatted

        return paths

    def format_list_data(self, datalist, freq, detail):
        reslist = []
        for d in datalist:
            reslist.append(self.format_single_data(d, freq, detail))
        return reslist

    def format_single_data(self, data, freq, detail):
        if 'aspath' not in data or data['aspath'] is None:
            return data

        if detail in ['matrix', 'basic', 'raw']:
            return data
 
        pathlen = 0
        aspath = []
        toquery = set()

        for asn in data['aspath']:
            asnsplit = asn.split('.')
            if len(asnsplit) != 2:
                continue
            
            if asnsplit[1] == "-2":
                aslabel = asname = "RFC 1918"
                
            elif asnsplit[1] == "-1":
                aslabel = asname = "No response"
            elif asnsplit[1] == "0":
                aslabel = asname = "Unknown"
            else:
                aslabel = "AS" + asnsplit[1]
                asname = None
                toquery.add(aslabel)
            
            repeats = int(asnsplit[0])
            pathlen += repeats 
            
            for i in range(0, repeats):
                aspath.append([asname, 0, aslabel])
        
        data['aspathlen'] = pathlen

        if len(toquery) == 0:
            data['aspath'] = aspath
            return data

        queried = self.asnmanager.queryASNames(toquery)
        if queried is None:
            log("Unable to query AS names")
            data['aspath'] = aspath
            return data

        for asp in aspath:
            if asp[0] != None:
                continue
            if asp[2] not in queried:
                asp[0] = asp[2]
            else:
                asp[0] = queried[asp[2]]

        data['aspath'] = aspath
        return data

    def get_maximum_view_groups(self):
        return 1

    def translate_group(self, groupprops):
        if 'aggregation' not in groupprops or groupprops['aggregation'] \
                    not in ["IPV4", "IPV6"]:
            return None

        return super(AmpTraceroute, self).translate_group(groupprops)


class AmpAsTraceroute(AmpTraceroute):
    def __init__(self, colid, viewmanager, nntscconf, asnmanager):
        super(AmpAsTraceroute, self).__init__(colid, viewmanager, nntscconf,
                asnmanager)
        self.collection_name = "amp-astraceroute"
        self.viewstyle = "amp-astraceroute"
        self.default_aggregation = "FAMILY"

    def get_maximum_view_groups(self):
        return 1

    def group_columns(self, detail):
        return []
    
    def detail_columns(self, detail):
        if detail == "matrix" or detail == "basic" or detail == "raw":
            aggfuncs = ["avg", "most_array"]
            aggcols = ["responses", "aspath"]
        elif detail == "hops-full" or detail == "hops-summary":
            aggfuncs = ["most_array"]
            aggcols = ["aspath"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["responses"]
        
        return aggcols, aggfuncs
   
    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        # We can only show one family on a graph at a time
        if groupparams['aggregation'] == "FAMILY":
            groupparams['aggregation'] = "IPV4"

        label = "%s to %s" % (groupparams['source'],
                groupparams['destination'])
        return label, self.splits[groupparams['aggregation']]
    
    def extra_blocks(self, detail):
        if detail == "hops-full" or detail == "full":
            return 2
        return 0

    def get_collection_history(self, cache, labels, start, end, detail,
            binsize):
        result = super(AmpAsTraceroute, self).get_collection_history(cache,
                            labels, start, end, detail, binsize)
        return result

    def translate_group(self, groupprops):
        if 'aggregation' not in groupprops or groupprops['aggregation'] \
                    not in ["IPV4", "IPV6"]:
            return None

        return super(AmpAsTraceroute, self).translate_group(groupprops)
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
