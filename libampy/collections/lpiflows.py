from libnntscclient.logger import *
from libampy.collection import Collection
from operator import itemgetter

class LPIFlows(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(LPIFlows, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'protocol', 'user', 'metric', 'dir']
        self.groupproperties = ['source', 'protocol', 'user', 'metric', 'dir']
        self.collection_name = "lpi-flows"
        self.diraggs = ["IN", "OUT", "BOTH"]
        self.supportedmetrics = ['new', 'peak']
        self.metric = "flows"

    def detail_columns(self, detail):
        return ['flows'], ['avg']

    def create_group_description(self, properties):
        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        # If properties describe a stream, we'll need to convert the
        # direction to upper case.
        properties['dir'] = properties['dir'].upper()

        return "MONITOR %s PROTOCOL %s USER %s METRIC %s %s" % \
                tuple([properties[x] for x in self.groupproperties])

    def parse_group_description(self, description):
        regex  = "MONITOR (?P<source>[.a-zA-Z0-9-]+) "
        regex += "PROTOCOL (?P<protocol>\S+) "
        regex += "USER (?P<user>\S+) "
        regex += "METRIC (?P<metric>[a-zA-Z0-9-]+) "
        regex += "(?P<direction>[A-Z]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('direction') not in self.diraggs:
            log("%s is not a valid direction for a %s group" % \
                    (parts.group('direction'), self.collection_name))
            return None

        if parts.group('metric') not in self.supportedmetrics:
            log("%s is not a valid metric for a %s group" % \
                    (parts.group('metric'), self.collection_name))
            return None

        keydict = {
            'source':parts.group('source'),
            'protocol':parts.group('protocol'),
            'user':parts.group('user'),
            'metric':parts.group('metric'),
            'dir':parts.group('direction')
        }
        return keydict

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s %s %s for %s at %s %s" % (groupparams['protocol'], \
                groupparams['metric'],
                self.metric, groupparams['user'], groupparams['source'],
                groupparams['dir'])
        return label

    def _generate_label(self, baselabel, search, direction, lookup):
        key = baselabel + "_" + direction
        search['dir'] =  direction
        shortlabel = direction

        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None
        else:
            streams = []

        return key, streams, shortlabel

    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate group labels")
            return None

        baselabel = 'group_%s' % (groupid)

        search = {'source':groupparams['source'],
                  'protocol':groupparams['protocol'],
                  'user':groupparams['user'],
                  'metric':groupparams['metric']
        }
        
        if groupparams['dir'] in ['IN', 'BOTH']:
            lab = self._generate_label(baselabel, search, 'in', lookup)
            if lab is None:
                return None
            
            labels.append({'labelstring':lab[0], 'streams':lab[1], 
                    'shortlabel':lab[2]})

        if groupparams['dir'] in ['OUT', 'BOTH']:
            lab = self._generate_label(baselabel, search, 'out', lookup)
            if lab is None:
                return None
            
            labels.append({'labelstring':lab[0], 'streams':lab[1], 
                    'shortlabel':lab[2]})

        return sorted(labels, key=itemgetter('shortlabel'))

    def translate_group(self, groupprops):
        if 'source' not in groupprops:
            return None
        if 'protocol' not in groupprops:
            return None

        if 'metric' not in groupprops or \
                groupprops['metric'] not in self.supportedmetrics:
            groupprops['metric'] = 'peak'

        if 'dir' not in groupprops:
            groupprops['dir'] = "BOTH"
        if 'user' not in groupprops:
            groupprops['user'] = 'all'

        return self.create_group_description(groupprops)



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
