from libnntscclient.logger import *
from libampy.collection import Collection
from operator import itemgetter

class LPIUsers(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(LPIUsers, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'protocol', 'metric']
        self.groupproperties = ['source', 'protocol', 'metric']
        self.collection_name = "lpi-users"
        self.aggmetrics = ['OBSERVED', "ACTIVE", "BOTH"]
        self.metric = "users"
        self.viewstyle = self.collection_name

    def detail_columns(self, detail):
        return ['users'], ['avg']

    def create_group_description(self, properties):
        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        # If properties describe a stream, we'll need to convert the
        # metric to upper case.
        properties['metric'] = properties['metric'].upper()

        return "MONITOR %s PROTOCOL %s %s" % \
                tuple([properties[x] for x in self.groupproperties])

    def parse_group_description(self, description):
        regex  = "MONITOR (?P<source>[.a-zA-Z0-9-]+) "
        regex += "PROTOCOL (?P<protocol>\S+) "
        regex += "(?P<metric>[A-Z]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('metric') not in self.aggmetrics:
            log("%s is not a valid metric for a %s group" % \
                    (parts.group('metric'), self.collection_name))
            return None

        keydict = {
            'source':parts.group('source'),
            'protocol':parts.group('protocol'),
            'metric':parts.group('metric'),
        }
        return keydict

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s %s at %s %s" % (groupparams['protocol'], \
                self.metric, groupparams['source'],
                groupparams['metric'])
        return label

    def _generate_label(self, baselabel, search, metric, lookup):
        key = baselabel + "_" + metric
        search['metric'] =  metric
        shortlabel = metric

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
                  'protocol':groupparams['protocol']
        }
        
        if groupparams['metric'] in ['OBSERVED', 'BOTH']:
            lab = self._generate_label(baselabel, search, 'observed', lookup)
            if lab is None:
                return None
            
            labels.append({'labelstring':lab[0], 'streams':lab[1], 
                    'shortlabel':lab[2]})

        if groupparams['metric'] in ['ACTIVE', 'BOTH']:
            lab = self._generate_label(baselabel, search, 'active', lookup)
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

        # If the collection we are coming from doesn't provide a suitable
        # metric, provide a sensible default
        if 'metric' not in groupprops:
            groupprops['metric'] = "BOTH"
        elif groupprops['metric'].upper() not in self.aggmetrics:
            groupprops['metric'] = "BOTH"


        return self.create_group_description(groupprops)



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
