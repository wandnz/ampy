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
        self.default_packet_sizes = ["64", "60"]
        self.viewstyle = 'amp-latency'
        self.integerproperties = ['port']

        self.portpreferences = [443, 53, 80]

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

        label = "%s to %s:%s TCP" % (groupparams['source'],
                groupparams['destination'], groupparams['port'])
        return label, self.splits[groupparams['aggregation']]

    def _group_to_search(self, groupparams):
        return {'source':groupparams['source'],
                'destination':groupparams['destination'],
                'port':int(groupparams['port']),
                'packet_size':groupparams['packet_size']}        

   
    def update_matrix_groups(self, source, dest, split, groups, views,
            viewmanager, viewstyle):

        baseprop = {'source':source, 'destination':dest}

        sels = self.streammanager.find_selections(baseprop, "", "1", 30000, False)
        if sels is None:
            return None

        req, ports = sels
        if req != 'port':
            log("Unable to find suitable ports for %s matrix cell %s to %s" \
                    % (self.collection_name, source, dest))
            return None

        if ports == {} or 'items' not in ports:
            views[(source, dest)] = -1
            return

        for p in self.portpreferences:
            if any(p == int(found['text']) for found in ports['items']):
                baseprop['port'] = p
                break

        if 'port' not in baseprop:
            # Just use the lowest port number for now
            ports.sort()
            baseprop['port'] = int(ports['items'][0]['text'])

        sels = self.streammanager.find_selections(baseprop, "", "1", 30000, False)
        if sels is None:
            return None


        # Find a suitable packet size, based on our test preferences
        if sels[0] != 'packet_size':
            log("Unable to find suitable packet sizes for %s matrix cell %s to %s" \
                    % (self.collection_name, source, dest))
            return None

        if sels[1] == {} or 'items' not in sels[1]:
            views[(source, dest)] = -1
            return

        for p in self.default_packet_sizes:
            if any(p == found['text'] for found in sels[1]['items']):
                baseprop['packet_size'] = p
                break

        if 'packet_size' not in baseprop:
            minsize = 0
            for s in sels[1]['items']:
                if s['text'] == "random":
                    continue
                try:
                    if int(s['text']) < minsize or minsize == 0:
                        minsize = int(s['text'])
                except TypeError:
                    # packet size is not an int, so ignore it
                    pass

            if minsize == 0:
                return None
            baseprop['packet_size'] = str(minsize)

        v4 = self._matrix_group_streams(baseprop, 'ipv4', groups)
        v6 = self._matrix_group_streams(baseprop, 'ipv6', groups)

        if v4 == 0 and v6 == 0:
            views[(source, dest)] = -1
            return

        if split == "ipv4":
            split = "IPV4"
        elif split == "ipv6":
            split = "IPV6"
        else:
            split = "FAMILY"

        cellgroup = self.create_group_from_list([source, dest, \
                baseprop['port'], baseprop['packet_size'], split])

        if cellgroup is None:
            log("Failed to create group for %s matrix cell" % \
                    (self.collection_name))
            return None

        viewid = viewmanager.add_groups_to_view(viewstyle,
                self.collection_name, 0, [cellgroup])
        if viewid is None:
            views[(source, dest)] = -1
        else:
            views[(source, dest)] = viewid



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
