from libampy.database import AmpyDatabase
from libnntscclient.logger import *

class AmpMesh(object):
    """ 
    Class for interacting with the AMP meta-data database.

    API Functions
    -------------
    get_sources:
        Returns a list of all sources belonging to a given mesh
    get_destinations:
        Returns a list of all destinations / targets belonging to a given mesh
    """

    def __init__(self, ampdbconfig):
        """ 
        Init function for the AmpMesh class.

        Parameters:
            
          ampdbconfig -- dictionary containing configuration parameters that
                describe how to connect to the meta-data database. See
                the AmpyDatabase class for details on the possible parameters.
        """
           
        # Default database name is 'amp2'                        
        if 'name' not in ampdbconfig:
            ampdbconfig['name'] = 'amp2'
        self.dbconfig = ampdbconfig
        self.db = AmpyDatabase(ampdbconfig, False)
        self.db.connect(15)

    def _meshquery(self, query, params):
        """ 
        Performs a basic query for mesh members and returns a list of results.

        Parameters:

          query -- the query to perform, as a parameterised string
          params -- a tuple containing parameters to substitute into the query

        Returns:
          a list of results returned by the query
        """
        sites = []

        if self.db.executequery(query, params) == -1:
            log("Error while querying sources for mesh %s" % (mesh))
            return None

        for row in self.db.cursor.fetchall():
            sites.append(row['ampname'])
        self.db.closecursor()
        return sites


    def get_sources(self, mesh):
        """
        Fetches all known sources that belong to the given mesh.

        Parameters:
          mesh -- the mesh to be queried

        Returns:
          a list of all sources belonging to the mesh
        """
        query = """ SELECT ampname FROM active_mesh_members WHERE
                    meshname=%s AND mesh_is_src = true ORDER BY ampname
                """
        params = (mesh,)
        return self._meshquery(query, params)

    def get_destinations(self, mesh):
        """
        Fetches all known targets that belong to the given mesh.

        Parameters:
          mesh -- the mesh to be queried

        Returns:
          a list of all targets belonging to the mesh
        """
        query = """ SELECT ampname FROM active_mesh_members WHERE
                    meshname=%s AND mesh_is_dst = true ORDER BY ampname
                """
        params = (mesh,)
        return self._meshquery(query, params)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
