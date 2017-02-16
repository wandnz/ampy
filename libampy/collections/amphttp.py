from libnntscclient.logger import log
from libampy.collection import Collection

class AmpHttp(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpHttp, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = [
            'source', 'destination', 'max_connections',
            'max_connections_per_server', 'persist',
            'max_persistent_connections_per_server', 'pipelining',
            'pipelining_max_requests', 'caching'
        ]
        self.groupproperties = [
            'source', 'destination', 'max_connections',
            'max_connections_per_server', 'persist',
            'max_persistent_connections_per_server', 'pipelining',
            'pipelining_max_requests', 'caching'
        ]
        self.integerproperties = [
            'max_connections', 'pipelining_max_requests',
            'max_persistent_connections_per_server',
            'max_connections_per_server'
        ]

        self.collection_name = "amp-http"
        self.viewstyle = self.collection_name

    def convert_property(self, streamprop, value):
        if streamprop == "destination":
            return value.replace("|", "/")
        return value

    def detail_columns(self, detail):
        """
        Determines which data table columns should be queried and how they
        should be aggregated, given the amount of detail required by the user.
        """
        if detail in ['matrix', 'basic', 'spark', 'tooltiptext']:
            aggs = ['avg', 'stddev', 'max', 'avg', 'stddev']
            cols = ['duration', 'duration', 'bytes', 'bytes', 'bytes']
        else:
            cols = ['server_count', 'object_count', 'duration', 'bytes']
            aggs = ['max', 'max', 'max', 'max']

        return cols, aggs

    def calculate_binsize(self, start, end, detail):
        """
        Determines an appropriate binsize for a graph covering the
        specified time period.
        """
        if (end - start) / 900 < 200:
            return 900

        if (end - start) / (900 * 4) < 200:
            return (900 * 4)

        if (end - start) / (900 * 12) < 200:
            return (900 * 12)

        return (900 * 24)

    def create_group_description(self, properties):
        """
        Converts a dictionary of stream or group properties into a string
        describing the group.
        """
        for prop in self.groupproperties:
            if prop not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (prop, self.collection_name))
                return None

            if prop == 'persist' and properties[prop] is True:
                properties[prop] = "PERSIST"
            elif prop == 'persist' and properties[prop] is False:
                properties[prop] = "NOPERSIST"

            if prop == 'pipelining' and properties[prop] is True:
                properties[prop] = "PIPELINING"
            elif prop == 'pipelining' and properties[prop] is False:
                properties[prop] = "NOPIPELINING"

            if prop == 'caching' and properties[prop] is True:
                properties[prop] = "CACHING"
            elif prop == 'caching' and properties[prop] is False:
                properties[prop] = "NOCACHING"

        return "FROM %s FETCH %s MC %s %s %s %s %s %s %s" % (
                    properties['source'], properties['destination'],
                    properties['max_connections'],
                    properties['max_connections_per_server'],
                    properties['persist'],
                    properties['max_persistent_connections_per_server'],
                    properties['pipelining'],
                    properties['pipelining_max_requests'],
                    properties['caching'])

    def parse_group_description(self, description):
        """
        Converts a group description string into a dictionary mapping
        group properties to their values.
        """
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
        """
        Converts a group description string into an appropriate label for
        placing on a graph legend.
        """
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        if groupparams["caching"] is True:
            cachelab = "+cached "
        else:
            cachelab = ""

        if groupparams["pipelining"] is True:
            pipelab = "+pipelining "
        else:
            pipelab = ""

        label = "%s from %s %s%s" % (groupparams['destination'],
                groupparams['source'], pipelab, cachelab)
        return label, ""

    def group_to_labels(self, groupid, description, lookup=True):
        """
        Converts a group description string into a set of labels describing
        each of the lines that would need to be drawn on a graph for that group.
        """
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        baselabel = 'group_%s' % (groupid)

        for key, value in groupparams.iteritems():
            if key in self.integerproperties:
                groupparams[key] = int(value)

        if lookup:
            streams = self.streammanager.find_streams(groupparams)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (baselabel, self.collection_name))
                return None
        else:
            streams = []

        labels.append({
            'labelstring': baselabel,
            'streams': streams,
            'shortlabel': '%s' % (groupparams['destination'])
        })

        return labels

    def update_matrix_groups(self, cache, source, dest, split, groups, views,
            viewmanager, viewstyle):
        """
        Finds all of the groups that need to queried to populate a matrix cell,
        including the stream ids of the group members.
        """
        groupprops = {'source': source, 'destination': dest}

        label = "%s_%s_ipv4" % (source, dest)
        streams = self.streammanager.find_streams(groupprops)

        if len(streams) == 0:
            views[(source, dest)] = -1
            return

        groups.append({'labelstring': label, 'streams': streams})

        cellgroups = []
        for stream in streams:
            props = self.streammanager.find_stream_properties(stream)

            proplist = [props[x] for x in self.groupproperties]
            cellgroup = self.create_group_from_list(proplist)

            if cellgroup is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                break
            cellgroups.append(cellgroup)

        cachelabel = "_".join([viewstyle, self.collection_name,
                source, dest])
        viewid = cache.search_matrix_view(cachelabel)
        if viewid is not None:
            views[(source, dest)] = viewid
            return

        viewid = viewmanager.add_groups_to_view(viewstyle,
                self.collection_name, 0, cellgroups)
        if viewid is None:
            views[(source, dest)] = -1
            cache.store_matrix_view(cachelabel, -1, 300)
        else:
            views[(source, dest)] = viewid
            cache.store_matrix_view(cachelabel, viewid, 0)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
