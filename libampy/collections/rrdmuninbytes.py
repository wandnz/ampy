from operator import itemgetter
from libnntscclient.logger import *
from libampy.collection import Collection

class RRDMuninbytes(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(RRDMuninbytes, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['switch', 'interfacelabel', 'direction']
        self.groupproperties = ['switch', 'interfacelabel', 'direction']
        self.collection_name = 'rrd-muninbytes'
        self.groupsplits = ["SENT", "RECEIVED", "BOTH"]
        self.viewstyle = self.collection_name

    def detail_columns(self, detail):
        return ["bytes"], ["avg"]

    def format_single_data(self, data, freq, detail):
        if "bytes" not in data:
            return data

        if data['bytes'] is None:
            data['mbps'] = None
        else:
            data['mbps'] = ((float(data['bytes']) * 8.0) / 1000000.0)

        return data

    def format_list_data(self, datalist, freq, detail):

        for d in datalist:
            self.format_single_data(d, freq, detail)
        return datalist

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s: %s" % \
                (groupparams['switch'], groupparams['interfacelabel'])
        return label, groupparams['direction']

    def create_group_description(self, properties):
        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        # If properties describe a stream, we'll need to convert the
        # direction to upper case.
        properties['direction'] = properties['direction'].upper()

        return "SWITCH-%s INTERFACE-%s %s" % \
                tuple([properties[x] for x in self.groupproperties])

    def parse_group_description(self, description):
        # Unlike most collections, we can't easily use regexs to parse
        # the description as our properties can be multiple words.

        props = {}

        interind = description.find(" INTERFACE-")
        dirind = description.rfind(" ")

        props['switch'] = description[len("SWITCH-"):interind]
        props['interfacelabel'] = description[interind + len(" INTERFACE-"):dirind]
        props['direction'] = description[dirind + 1:]

        if props['direction'] not in self.groupsplits:
            return None

        return props

    def _generate_label(self, baselabel, search, direction, lookup):
        key = baselabel + "_" + direction
        search['direction'] = direction
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

        search = {'switch':groupparams['switch'],
                  'interfacelabel':groupparams['interfacelabel']
        }

        if groupparams['direction'] in ['SENT', 'BOTH']:
            sentlabel = self._generate_label(baselabel, search, "sent", lookup)
            if sentlabel is None:
                return None

            labels.append({'labelstring':sentlabel[0], 'streams':sentlabel[1],
                    'shortlabel':sentlabel[2]})

        if groupparams['direction'] in ['RECEIVED', 'BOTH']:
            recvlabel = self._generate_label(baselabel, search, "received",
                    lookup)
            if recvlabel is None:
                return None

            labels.append({'labelstring':recvlabel[0], 'streams':recvlabel[1],
                    'shortlabel':recvlabel[2]})

        return sorted(labels, key=itemgetter('shortlabel'))



# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
