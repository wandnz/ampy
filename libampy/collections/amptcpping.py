from libnntscclient.logger import *
from libampy.collections.ampicmp import AmpIcmp
from operator import itemgetter

class AmpTcpping(AmpIcmp):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpTcpping, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'destination', 'port', \
                'packet_size', 'family']
        self.groupproperties = ['source', 'destination', 'port', \
                'packet_size', 'aggregation']
        self.collection_name = 'amp-tcpping'
        self.default_packet_size = 60
        self.viewstyle = 'amp-latency'
        self.integerproperties = ['port']

    def create_group_description(self, properties):

        if 'family' in properties:
            properties['aggregation'] = properties['family'].upper()

        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                    (p, self.collection_name))
                return None

        return "FROM %s TO %s PORT %s SIZE %s %s" % ( \
                properties['source'], properties['destination'],
                properties['port'], 
                properties['packet_size'], properties['aggregation'].upper()) 

    def parse_group_description(self, description):
        regex =  "FROM (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9-]+) "
        regex += "PORT (?P<port>[0-9]+) "
        regex += "SIZE (?P<size>[a-zA-Z0-9]+) "
        regex += "(?P<split>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)

        if parts is None:
            return None

        if parts.group("split") not in self.splits:
            log("%s group description has no aggregation method" % \
                    (self.collection_name))
            log(description)
            return None

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "port": parts.group("port"),
            "packet_size": parts.group("size"),
            "aggregation": parts.group("split")
        }

        return keydict

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return none

        label = "%s to %s:%s %s" % (groupparams['source'],
                groupparams['destination'], groupparams['port'],
                self.splits[groupparams['aggregation']])
        return label

    def _group_to_search(self, groupparams):
        return {'source':groupparams['source'],
                'destination':groupparams['destination'],
                'port':int(groupparams['port']),
                'packet_size':groupparams['packet_size']}        

   
    def update_matrix_groups(self, source, dest, groups, views, viewmanager): 

        baseprop = {'source':source, 'destination':dest}
        
        sels = self.streammanager.find_selections(baseprop)
        if sels is None:
            return None
        
        req, ports = sels
        if req != 'port':
            log("Unable to find suitable ports for %s matrix cell %s to %s" \
                    % (self.collection_name, source, dest))
            return None
        
        if ports == []:
            views[(source, dest)] = -1
            return
        
        # Just use the lowest port number for now
        baseprop['port'] = int(ports.sort()[0] )
        baseprop['packet_size'] = self.default_packet_size

        v4 = self._matrix_group_streams(groupprops, 'ipv4', groups)
        v6 = self._matrix_group_streams(groupprops, 'ipv6', groups)

        if v4 == 0 and v6 == 0:
            views[(source, dest)] = -1
            return

        cellgroup = self.create_group_from_list([source, dest, \
                baseprop['port'], self.default_packet_size, "FAMILY"])

        if cellgroup is None:
            log("Failed to create group for %s matrix cell" % \
                    (self.collection_name))
            return None

        viewid = viewmanager.add_groups_to_view(self.viewstyle,
                self.collection_name, 0, [cellgroup])
        if viewid is None:
            views[(source, dest)] = -1
        else:
            views[(source, dest)] = viewid



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
