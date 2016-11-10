from libnntscclient.logger import *
from libampy.collection import Collection

class AmpHttp(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpHttp, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = ['source', 'destination', 'max_connections',
                'max_connections_per_server', 'persist',
                'max_persistent_connections_per_server', 'pipelining',
                'pipelining_max_requests', 'caching']
        self.groupproperties = ['source', 'destination', 'max_connections',
                'max_connections_per_server', 'persist',
                'max_persistent_connections_per_server', 'pipelining',
                'pipelining_max_requests', 'caching']

        self.integerproperties = ['max_connections', 'pipelining_max_requests',
                'max_persistent_connections_per_server',
                'max_connections_per_server']

        self.collection_name = "amp-http"
        self.viewstyle = self.collection_name

    def convert_property(self, streamprop, value):
        if streamprop == "destination":
            return value.replace("|", "/")
        return value

    def detail_columns(self, detail):
        if detail in ['matrix', 'basic', 'spark', 'tooltiptext']:
            aggs = ['avg', 'max']
            cols = ['duration', 'bytes']
        else:
            cols = ['server_count', 'object_count', 'duration', 'bytes']
            aggs = ['max', 'max', 'max', 'max']

        return cols, aggs

    def calculate_binsize(self, start, end, detail):
        if (end - start) / 900 < 200:
            return 900

        if (end - start) / (900 * 4) < 200:
            return (900 * 4)

        if (end - start) / (900 * 12) < 200:
            return (900 * 12)

        return (900 * 24)

    def create_group_description(self, properties):
        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

            if p == 'persist' and properties[p] == True:
                properties[p] = "PERSIST"
            elif p == 'persist' and properties[p] == False:
                properties[p] = "NOPERSIST"

            if p == 'pipelining' and properties[p] == True:
                properties[p] = "PIPELINING"
            elif p == 'pipelining' and properties[p] == False:
                properties[p] = "NOPIPELINING"

            if p == 'caching' and properties[p] == True:
                properties[p] = "CACHING"
            elif p == 'caching' and properties[p] == False:
                properties[p] = "NOCACHING"

        return "FROM %s FETCH %s MC %s %s %s %s %s %s %s" % \
                (properties['source'], properties['destination'],
                    properties['max_connections'],
                    properties['max_connections_per_server'],
                    properties['persist'],
                    properties['max_persistent_connections_per_server'],
                    properties['pipelining'],
                    properties['pipelining_max_requests'],
                    properties['caching'])

    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9-]+) "
        regex += "FETCH (?P<destination>[\S]+) "
        regex += "MC (?P<maxconn>[0-9]+) (?P<maxconnserver>[0-9]+) "
        regex += "(?P<persist>[A-Z]+) "
        regex += "(?P<maxpersistconn>[0-9]+) "
        regex += "(?P<pipeline>[A-Z]+) "
        regex += "(?P<maxpipeline>[0-9]+) "
        regex += "(?P<caching>[A-Z]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        keydict = {
            'source': parts.group('source'),
            'destination': parts.group('destination'),
            'max_connections': int(parts.group('maxconn')),
            'max_connections_per_server': int(parts.group('maxconnserver')),
            'max_persistent_connections_per_server': int(parts.group('maxpersistconn')),
            'pipelining_max_requests': int(parts.group('maxpipeline')),
            'persist': False,
            'caching': False,
            'pipelining': False
        }

        if parts.group('persist') == "PERSIST":
            keydict['persist'] = True
        if parts.group('pipeline') == "PIPELINING":
            keydict['pipelining'] = True
        if parts.group('caching') == "CACHING":
            keydict['caching'] = True

        return keydict

    def get_legend_label(self, description):
        gps = self.parse_group_description(description)
        if gps is None:
            log("Failed to parse group description to generate legend label")
            return None

        if gps["caching"] == True:
            cachelab = "+cached "
        else:
            cachelab = ""

        if gps["pipelining"] == True:
            pipelab = "+pipelining "
        else:
            pipelab = ""

        label = "%s from %s %s%s" % (gps['destination'], gps['source'], \
                pipelab, cachelab)
        return label, ""


    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        gps = self.parse_group_description(description)
        if gps is None:
            log("Failed to parse group description to generate legend label")
            return None

        baselabel = 'group_%s' % (groupid)

        for k, v in gps.iteritems():
            if k in self.integerproperties:
                gps[k] = int(v)


        if lookup:
            streams = self.streammanager.find_streams(gps)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (baselabel, self.collection_name))
                return None
        else:
            streams = []

        labels.append({'labelstring':baselabel, 'streams':streams,
                'shortlabel': '%s' % (gps['destination'])})

        return labels

    def update_matrix_groups(self, source, dest, split, groups, views,
            viewmanager, viewstyle):
        groupprops = {'source': source, 'destination': dest}

        label = "%s_%s_ipv4" % (source, dest)
        streams = self.streammanager.find_streams(groupprops)

        if len(streams) == 0:
            views[(source, dest)] = -1
            return

        groups.append({'labelstring':label, 'streams':streams})

        cgs = []
        for s in streams:
            props = self.streammanager.find_stream_properties(s)

            proplist = [props[x] for x in self.groupproperties]
            cellgroup = self.create_group_from_list(proplist)

            if cellgroup is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                break
            cgs.append(cellgroup)

        viewid = viewmanager.add_groups_to_view(viewstyle,
                self.collection_name, 0, cgs)
        if viewid is None:
            views[(source, dest)] = -1
        else:
            views[(source, dest)] = viewid

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
