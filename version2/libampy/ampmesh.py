from libampy.database import AmpyDatabase
from libnntscclient.logger import *

class AmpMesh(object):

    def __init__(self, ampdbconfig):
        if 'name' not in ampdbconfig:
            ampdbconfig['name'] = 'amp2'
        self.dbconfig = ampdbconfig
        self.db = AmpyDatabase(ampdbconfig, True)
        self.db.connect(15)

    def _meshquery(self, query, params):
        sites = []

        if self.db.executequery(query, params) == -1:
            log("Error while querying sources for mesh %s" % (mesh))
            return None

        for row in self.db.cursor.fetchall():
            sites.append(row['ampname'])
        return sites


    def get_sources(self, mesh):
        query = """ SELECT ampname FROM active_mesh_members WHERE
                    meshname=%s AND mesh_is_src = true ORDER BY ampname
                """
        params = (mesh,)
        return self._meshquery(query, params)

    def get_destinations(self, mesh):
        query = """ SELECT ampname FROM active_mesh_members WHERE
                    meshname=%s AND mesh_is_dst = true ORDER BY ampname
                """
        params = (mesh,)
        return self._meshquery(query, params)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
