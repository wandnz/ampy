from libampy.database import AmpyDatabase
from libnntscclient.logger import *
from threading import Lock

class AmpMesh(object):
    """
    Class for interacting with the AMP meta-data database.

    API Functions
    -------------
    get_mesh_sources:
        Returns a list of all sources belonging to a given mesh
    get_mesh_destinations:
        Returns a list of all destinations / targets belonging to a given mesh
    get_meshes:
        Returns a list of available source or destination meshes
    get_sources:
        Returns a list of all available source sites
    get_destinations:
        Returns a list of all available destination sites
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
        self.db = AmpyDatabase(ampdbconfig, True)
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


    def get_mesh_sources(self, mesh):
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

    def get_mesh_destinations(self, mesh):
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

    def get_meshes(self, endpoint, site=None):
        """
        Fetches all source or destination meshes.

        Parameters:
          endpoint -- either "source" or "destination", depending on
                      which meshes are required.
          site -- optional argument to filter only meshes that this
                  site is a member of.

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
        params = None
        table = "mesh"

        if site is not None:
            # if site is set then need to do a join to get only meshes
            # that site belongs to
            params = (site,)
            table = """ active_mesh_members JOIN mesh
                        ON active_mesh_members.meshname = mesh.mesh_name
                    """

        query = """ SELECT mesh_name, mesh_longname, mesh_description
                        FROM %s WHERE """ % table

        if site is not None:
            query += "ampname = %s"
        else:
            query += "mesh_active = true"

        if endpoint == "source":
            query += " AND mesh.mesh_is_src = true"
        elif endpoint == "destination":
            query += " AND mesh.mesh_is_dst = true"

        query += " ORDER BY mesh_longname"

        # If the endpoint is invalid, we'll currently return all meshes.
        # XXX Is this the correct behaviour?
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
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

    def _sitequery(self, query, params):
        """
        Performs a basic query for sites and returns a list of results.

        Parameters:

          query -- the query to perform, as a parameterised string
          params -- a tuple containing parameters to substitute into the query

        Returns:
          a list of results returned by the query
        """
        sites = []

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying for sites")
            self.dblock.release()
            return None

        for row in self.db.cursor.fetchall():
            sites.append({'ampname':row[0], 'longname':row[1], \
                    'location':row[2], 'description':row[3]})

        self.db.closecursor()
        self.dblock.release()
        return sites

    def get_sources(self):
        """
        Fetches all known sources.

        Parameters:
          None

        Returns:
          a list of all sources
        """
        query = """ SELECT DISTINCT site_ampname AS ampname,
                    site_longname AS longname,
                    site_location AS location, site_description AS description
                    FROM site JOIN active_mesh_members ON
                    site.site_ampname = active_mesh_members.ampname
                    WHERE mesh_is_src = true ORDER BY longname """

        return self._sitequery(query, None)

    def get_destinations(self):
        """
        Fetches all known destinations.

        Parameters:
          None

        Returns:
          a list of all destinations
        """
        query = """ SELECT DISTINCT site_ampname AS ampname,
                    site_longname AS longname,
                    site_location AS location, site_description AS description
                    FROM site JOIN active_mesh_members ON
                    site.site_ampname = active_mesh_members.ampname
                    WHERE mesh_is_dst = true ORDER BY longname """

        return self._sitequery(query, None)

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

    # TODO move schedule stuff into a specific schedule source file?
    def schedule_new_test(self, src, dst, test, freq, start, end, period, args):
        query = """ INSERT INTO schedule (schedule_test, schedule_frequency,
                    schedule_start, schedule_end, schedule_period,
                    schedule_args, schedule_modified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING schedule_id """
        # TODO sanity check arguments? make sure test exists etc
        params = (test, freq, start, end, period, args, int(time.time()))

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while scheduling new test")
                self.dblock.release()
                return None

        schedule_id = self.db.cursor.fetchone()['schedule_id']
        self.db.closecursor()
        self.dblock.release()
        print "added schedule with id", schedule_id

        # add the initial set of endpoints for this test
        self.add_endpoints_to_test(schedule_id, src, dst)

        return schedule_id

    def update_test(self, schedule_id, test, freq, start, end, period, args):
        query = """ UPDATE schedule SET schedule_test=%s,
                    schedule_frequency=%s, schedule_start=%s,
                    schedule_end=%s, schedule_period=%s, schedule_args=%s,
                    schedule_modified=%s WHERE schedule_id=%s """
        params = (test, freq, start, end, period, args, int(time.time()),
                schedule_id)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def delete_test(self, schedule_id):
        query = """ DELETE FROM schedule WHERE schedule_id=%s """
        params = (schedule_id,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while deleting scheduled test")
                self.dblock.release()
                return None

        self.db.closecursor()
        self.dblock.release()
        return True

    def _is_mesh(self, name):
        query = """ SELECT COUNT(*) FROM mesh WHERE mesh_name = %s """
        params = (name,)
        self.dblock.acquire()
        print query
        if self.db.executequery(query, params) == -1:
                log("Error while querying is_mesh()")
                self.dblock.release()
                return None

        count = self.db.cursor.fetchone()['count']
        print "is_mesh:", name, count
        self.db.closecursor()
        self.dblock.release()
        return count

    def _is_site(self, name):
        query = """ SELECT COUNT(*) FROM site WHERE site_ampname = %s """
        params = (name,)
        self.dblock.acquire()
        print query
        if self.db.executequery(query, params) == -1:
                log("Error while querying is_site()")
                self.dblock.release()
                return None

        count = self.db.cursor.fetchone()['count']
        print "is_site:", name, count
        self.db.closecursor()
        self.dblock.release()
        return count

    def _add_basic_site(self, name):
        query = """ INSERT INTO site (site_ampname, site_longname)
                    VALUES (%s, %s) """
        params = (name, name)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while inserting new site")
                self.dblock.release()
                return None

        self.db.closecursor()
        self.dblock.release()
        return True


    def add_endpoints_to_test(self, schedule_id, src, dst):
        # TODO avoid duplicate rows - set unique constraint?
        query = """ INSERT INTO endpoint (endpoint_schedule_id,
                    endpoint_source_mesh, endpoint_source_site,
                    endpoint_destination_mesh, endpoint_destination_site)
                    VALUES (%s, %s, %s, %s, %s) """

        print "adding endpoints to test"
        print src, dst, schedule_id

        print "checking source"
        if self._is_mesh(src):
            src_mesh = src
            src_site = None
        elif self._is_site(src):
            src_site = src
            src_mesh = None
        else:
            print "source is neither mesh nor site"
            return

        print "checking dest"
        if self._is_mesh(dst):
            dst_mesh = dst
            dst_site = None
        elif self._is_site(dst):
            dst_site = dst
            dst_mesh = None
        else:
            # assume the destination is a site that was entered with the
            # text input box, and create it if it doesn't exist
            dst_site = dst
            dst_mesh = None
            if self._add_basic_site(dst) is None:
                return

        params = (schedule_id, src_mesh, src_site, dst_mesh, dst_site)
        print params

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while inserting new test endpoints")
                self.dblock.release()
                return None
        self.db.closecursor()
        self._update_last_modified_schedule(schedule_id)
        self.dblock.release()
        return True # XXX

    def delete_endpoints(self, schedule_id, src, dst):
        query = """ DELETE FROM endpoint WHERE endpoint_schedule_id=%s """

        if self._is_mesh(src):
            query += " AND endpoint_source_mesh=%s"
        elif self._is_site(src):
            query += " AND endpoint_source_site=%s"
        else:
            print "source is neither mesh nor site"
            return

        if self._is_mesh(dst):
            query += " AND endpoint_destination_mesh=%s"
        elif self._is_site(dst):
            query += " AND endpoint_destination_site=%s"
        else:
            print "source is neither mesh nor site"
            return

        params = (schedule_id, src, dst)
        print query % params
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while deleting endpoints")
                self.dblock.release()
                return None

        self.db.closecursor()
        self._update_last_modified_schedule(schedule_id)
        self.dblock.release()

        return True

    # XXX expects the db lock to be held by caller, so we can update
    # this at the same time as the modifications get made
    def _update_last_modified_schedule(self, schedule_id):
        # update last modified time of schedule
        query = "UPDATE schedule SET schedule_modified=%s WHERE schedule_id=%s"
        params = (int(time.time()), schedule_id)

        # XXX can we ROLLBACK if this failed? or does autocommit screw us?
        if self.db.executequery(query, params) == -1:
            log("Error updating schedule modification time")
            self.dblock.release()
            return None

        self.db.closecursor()
        return True

    def get_source_schedule(self, source, schedule_id=None):
        """
        Fetch all scheduled tests that originate at this source

        Parameters:
          source -- the name of the source site/mesh to fetch the schedule for

        Returns:
          a list containing the scheduled tests from this source
        """
        if self._is_mesh(source):
            where = "endpoint_source_mesh=%s"
        elif self._is_site(source):
            where = "endpoint_source_site=%s"
        else:
            return None

        params = (source,)
        if schedule_id is not None:
            where += " AND schedule_id=%s"
            params = (source, schedule_id)
        query = """ SELECT schedule_id, schedule_test, schedule_frequency,
                    schedule_start, schedule_end, schedule_period,
                    schedule_args, max(schedule_modified) AS schedule_modified,
                    string_agg(endpoint_destination_mesh, ',') AS dest_mesh,
                    string_agg(endpoint_destination_site, ',') AS dest_site
                    FROM endpoint JOIN schedule
                    ON schedule_id=endpoint_schedule_id
                    WHERE %s GROUP BY schedule_id """ % where

        schedule = []

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying for schedule")
            self.dblock.release()
            return None

        for row in self.db.cursor.fetchall():
            # TODO need to know if source is single or part of a mesh test
            meshes = [] if row[8] is None else row[8].split(",")
            sites = [] if row[9] is None else row[9].split(",")
            schedule.append({'id':row[0], 'test':row[1], \
                    'frequency':row[2], 'start':row[3], \
                    'end':row[4], 'period':row[5], 'args':row[6],
                    'modified':row[7],
                    #'endpoints':endpoints})
                    'dest_mesh':meshes, 'dest_site':sites})

        self.db.closecursor()
        self.dblock.release()
        return schedule


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
