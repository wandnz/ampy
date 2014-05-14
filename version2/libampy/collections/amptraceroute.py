from libnntscclient.logger import *
from libampy.collection import Collection
from libampy.collections.ampicmp import AmpIcmp
import re
from operator import itemgetter

class AmpTraceroute(AmpIcmp):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpTraceroute, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'destination', 'packet_size',
                'family', 'address']
        self.groupproperties = ['source', 'destination', 'packet_size',
                'aggregation', 'address']
        self.collection_name = "amp-traceroute"
        self.default_packet_size = "60"

    def detail_columns(self, detail):
        if detail == "matrix":
            aggfuncs = ["avg"]
            aggcols = ["length"]
        elif detail == "hops":
            aggfuncs = ["most_array"]
            aggcols = ["path"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["length"]
        
        return aggcols, aggfuncs
    
    def group_to_labels(self, groupid, description, lookup=True):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate labels")
            return None

        # Amp Icmp handles these aggregation methods already
        if groupparams['aggregation'] in ['IPV4', 'FAMILY', 'FULL', 'IPV6',
                'NONE']:
            return super(AmpTraceroute, self).group_to_labels(groupid, 
                    description, lookup)

        baselabel = 'group_%s' % (groupid)
        search = {'source':groupparams['source'],
                'destination':groupparams['destination'],
                'packet_size':groupparams['packet_size']}

        labels = []

        if groupparams['aggregation'] in ['ADDRESS']:
            search['family'] = self._address_to_family(groupparams['address'])
            search['address'] = groupparams['address']
            
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for %s, %s" % \
                        (baselabel, self.collection_name))
                return None

            for sid,_ in streams:
                nextlab = {'labelstring':baselabel, 'streams':[sid], 
                        'shortlabel':search['address']}
                labels.append(nextlab)
       
        return sorted(labels, key=itemgetter('shortlabel'))
        
    def create_group_description(self, properties):

        # Only need to handle ADDRESS aggregation in here, all others can
        # fall through to AmpIcmp
        if 'aggregation' not in properties or properties['aggregation'] != \
                    'ADDRESS':
            return super(AmpTraceroute, self).create_group_description(properties)

    
        # Check that we have everything we need
        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                    (p, self.collection_name))
                return None
        
        return "FROM %s TO %s OPTION %s ADDRESS %s" % ( \
                 properties['source'], properties['destination'],
                 properties['packet_size'],  properties['address'])


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
