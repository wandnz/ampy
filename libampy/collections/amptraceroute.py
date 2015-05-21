from libnntscclient.logger import *
from libampy.collection import Collection
from libampy.collections.ampicmp import AmpIcmp
import re, socket
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
        self.default_aggregation = "FAMILY"
        self.viewstyle = "amp-traceroute"

        self.localcache = None
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

        # Save the cache because we'll want it for our AS name lookups    
        self.localcache = cache
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

    def format_list_data(self, datalist, freq, detail):
        reslist = []
        for d in datalist:
            reslist.append(self.format_single_data(d, freq, detail))
        return reslist

    def format_single_data(self, data, freq, detail):
        if 'aspath' not in data:
            return data

        if detail in ['matrix', 'basic']:
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
                asname = self.localcache.search_asname(aslabel)
                if asname == None:
                    toquery.add(aslabel)
            
            repeats = int(asnsplit[0])
            pathlen += repeats 
            
            for i in range(0, repeats):
                aspath.append([asname, 0, aslabel])
        
        data['aspathlen'] = pathlen

        if len(toquery) == 0:
            data['aspath'] = aspath
            return data

        queried = self._query_asnames(toquery)
        if queried is None:
            log("Unable to query AS names")
            data['aspath'] = aspath
            return data

        for asp in aspath:
            if asp[0] != None:
                continue
            asp[0] = queried[asp[2]]
        
        data['aspath'] = aspath
        return data

    def _query_asnames(self, toquery):
        if len(toquery) == 0:
            return {}

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)

        try:
            s.connect(('whois.cymru.com', 43))
        except socket.error, msg:
            log("Failed to connect to whois.cymru.com:43, %s" % (msg[1]))
            s.close()
            return {}

        msg = "begin\n"
        for q in toquery:
            msg += q + "\n"
        msg += "end\n"

        totalsent = 0
        while totalsent < len(msg):
            sent = s.send(msg[totalsent:])
            if sent == 0:
                log("Error while sending query to whois.cymru.com")
                s.close()
                return {}
            totalsent += sent

        # Receive all our responses
        responded = 0
        recvbuf = ""
        asnames = {}

        inds = list(toquery)
        while responded < len(toquery):
            chunk = s.recv(2048)
            if chunk == '':
                break
            recvbuf += chunk

            if '\n' not in recvbuf:
                continue

            lines = recvbuf.splitlines(True)
            consumed = 0
            for l in lines:
                if l[-1] == "\n":
                    if "Bulk mode" not in l:
                        asnames[inds[responded]] = l.strip()
                        self.localcache.store_asname(inds[responded], l.strip())
                        responded += 1
                    consumed += len(l)
            recvbuf = recvbuf[consumed:]
        s.close()

        return asnames


class AmpAsTraceroute(AmpTraceroute):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpAsTraceroute, self).__init__(colid, viewmanager, nntscconf)
        self.collection_name = "amp-astraceroute"
        self.viewstyle = "amp-astraceroute"
        self.default_aggregation = "FAMILY"

    def group_columns(self, detail):
        return []
    
    def detail_columns(self, detail):
        if detail == "matrix" or detail == "basic":
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

        label = "%s to %s %s" % (groupparams['source'],
                groupparams['destination'],
                self.splits[groupparams['aggregation']])
        return label
    
    def extra_blocks(self, detail):
        if detail == "hops-full" or detail == "full":
            return 2
        return 0

    def get_collection_history(self, cache, labels, start, end, detail,
            binsize):
        result = super(AmpAsTraceroute, self).get_collection_history(cache,
                            labels, start, end, detail, binsize)
        return result


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
