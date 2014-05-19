from libnntscclient.logger import *
from libampy.collection import Collection
from operator import itemgetter

class RRDSmokeping(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(RRDSmokeping, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'host']
        self.groupproperties = ['source', 'host']
        self.collection_name = "rrd-smokeping"

    def detail_columns(self, detail):
        if detail == "minimal":
            aggcols = ["median", "loss"]
            aggfuncs = ["avg", "avg"]
        else:
            aggcols = ["loss", "median", "pings"]
            aggfuncs = ["avg", "avg", "smokearray"]

        return aggcols, aggfuncs

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s to %s" % \
                tuple([groupparams[x] for x in self.groupproperties])
        return label

    def create_group_description(self, properties):
        
        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        return "SOURCE %s TARGET %s" % \
                tuple([properties[x] for x in self.groupproperties])

    def parse_group_description(self, description):
    
        regex = "SOURCE (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TARGET (?P<host>\S+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        keydict = {
            'source':parts.group('source'),
            'host':parts.group('host')
        }
        return keydict

    def group_to_labels(self, groupid, description, lookup=True):
        
        label = "group_%s" % (groupid)

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate labels")
            return None

        if lookup:
            streams = self.streammanager.find_streams(groupparams)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None
            # No storage included with the streams so we should already have
            # a list of stream IDs
        else:
            streams = []

        return [{'labelstring':label, 'streams':streams, \
                'shortlabel':groupparams['host']}]
       
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
