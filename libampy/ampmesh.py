from libampy.database import AmpyDatabase
from libnntscclient.logger import *
from threading import Lock

class AmpMesh(object):
    """ 
    Class for interacting with the AMP meta-data database.

    API Functions
    -------------
    get_sources:
        Returns a list of all sources belonging to a given mesh
    get_destinations:
        Returns a list of all destinations / targets belonging to a given mesh
    get_meshes:
        Returns a list of available source or destination meshes
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
        self.dblock = Lock()

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

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying sources for mesh %s" % (mesh))
            self.dblock.release()
            return None

        for row in self.db.cursor.fetchall():
            sites.append(row['ampname'])
        self.db.closecursor()
        self.dblock.release()
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

    def get_meshes(self, endpoint):
        """
        Fetches all source or destination meshes.

        Parameters:
          endpoint -- either "source" or "destination", depending on 
                      which meshes are required.

        Returns:
          a list of dictionaries that describe the available meshes or 
          None if an error occurs while querying for the meshes.

        Mesh dictionary format:
          The returned dictionaries should contain three elements:
            name -- the internal unique identifier string for the mesh
            longname -- a string containing a mesh name that is more 
                        suited for public display
            description -- a string describing the purpose of the mesh in
                           reasonable detail
        """
        query = """ SELECT mesh_name, mesh_longname, mesh_description
                        FROM mesh WHERE mesh_active = true
                    """

        if endpoint == "source":
            query += " AND mesh_is_src = true"
        elif endpoint == "destination":
            query += " AND mesh_is_dst = true"
        
        # If the endpoint is invalid, we'll currently return all meshes.
        # XXX Is this the correct behaviour?
        self.dblock.acquire()
        if self.db.executequery(query, None) == -1:
            log("Error while querying %s meshes" % (endpoint))
            self.dblock.release()
            return None

        meshes = []
        for row in self.db.cursor.fetchall():
            meshes.append({'name':row[0], 'longname':row[1], \
                    'description':row[2]})
        self.db.closecursor()
        self.dblock.release()
        return meshes

    def get_site_info(self, site):
        """
        Fetches details about a particular mesh member.

        Parameters:
          site -- the name of the mesh member to query for

        Returns: 
          a dictionary containing detailed information about the site.

        The resulting dictionary contains the following items:
          ampname -- a string containing the internal name for the site
          longname -- a string containing a site name that is suitable for
                      public display
          location -- a string containing the city or data-centre where the
                      amplet is located (if there is one for that site)
          description -- a string containing any additional descriptive
                         information about the site
          active -- a boolean flag indicating whether the site is currently
                    active
        """
        # Dummy dictionary in case we can't find the site in question
        unknown = {
            "ampname":site,
            "longname":site,
            "description":"",
            "location":"Location unknown",
            "active":False
        }

        query = """ SELECT site_ampname as ampname, site_longname as longname,
                    site_location as location, site_description as description,
                    site_active as active FROM site WHERE site_ampname = %s
                """
        params = (site,)
        
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying for site %s" % (site))
            self.dblock.release()
            return None

        result = self.db.cursor.fetchone()
        if result is None:
            log("Warning: unable to find site %s in amp database" % (site))
            self.db.closecursor()
            self.dblock.release()
            return unknown

        retdict = dict(result)
        self.db.closecursor()
        self.dblock.release()
        return retdict
         

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
