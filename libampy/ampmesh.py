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

    def get_meshes(self, endpoint, amptest=None, site=None):
        """
        Fetches all source or destination meshes.

        Parameters:
          endpoint -- either "source" or "destination", depending on
                      which meshes are required.
          amptest -- limit results to meshes that are targets for a given test.
                     If None, no filtering of meshes is performed. This
                     parameter is ignored if querying for source meshes.
                     Possible values include 'latency', 'hops', 'dns', 'http'
                     and 'tput'.
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
        params = []

        if amptest:
            # if a test is set then use the view that includes tests
            meshname = "active_mesh_members.meshname"
            table = """ active_mesh_members JOIN full_mesh_details
                ON active_mesh_members.meshname = full_mesh_details.meshname """
        else:
            # otherwise use all possible meshes, even if they have no tests
            meshname = "mesh_name"
            table = """ active_mesh_members RIGHT JOIN mesh
                ON active_mesh_members.meshname = mesh.mesh_name """

        # XXX count isnt always sensible? if WHERE is involved
        query = """ SELECT %s as mesh_name, mesh_longname,
                    mesh_description, count(active_mesh_members.*)
                    FROM %s WHERE mesh_active = true """ % (meshname, table)

        # if site is set then only return meshes that it belongs to
        if site is not None:
            query += " AND ampname = %s "
            params.append(site)

        # if test is set then only return destination meshes that the test is
        # performed to (ignored for source meshes)
        if amptest is not None and endpoint == "destination":
            query += " AND meshtests_test = %s "
            params.append(amptest)

        # return meshes of the appropriate type - source or dest
        if endpoint == "source":
            query += " AND active_mesh_members.mesh_is_src = true "
        elif endpoint == "destination":
            query += " AND active_mesh_members.mesh_is_dst = true"
        else:
            # for now just return source and destination meshes if no endpoint
            # is set, though this doesn't play that well with amptest being set
            pass

        query += " GROUP BY mesh_name, mesh_longname, mesh_description ORDER BY mesh_longname"

        self.dblock.acquire()
        if self.db.executequery(query, tuple(params)) == -1:
            log("Error while querying %s meshes" % (endpoint))
            self.dblock.release()
            return None

        meshes = []
        for row in self.db.cursor.fetchall():
            meshes.append({'ampname':row[0], 'longname':row[1], \
                    'description':row[2], 'count':row[3]})

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

    # XXX why do sites have to be in a mesh to count as a src/dst?
    def get_meshless_sites(self):
        """
        Fetches all sites that are not currently in a mesh
        """
        query = """ SELECT * FROM site WHERE site_ampname NOT IN (
                    SELECT ampname FROM active_mesh_members) """

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
            "ampname": site,
            "longname": site,
            "description": "",
            "location": "Location unknown",
            "active": False,
            "unknown": True
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
            #log("Warning: unable to find site %s in amp database" % (site))
            self.db.closecursor()
            self.dblock.release()
            return unknown

        retdict = dict(result)
        self.db.closecursor()
        self.dblock.release()
        return retdict

    def get_mesh_info(self, mesh):
        """ Get more detailed and human readable information about a mesh """
        # Dummy dictionary in case we can't find the site in question
        unknown = {
            "meshname": mesh,
            "longname": mesh,
            "description": "",
            "src": False,
            "dst": False,
            "active": False,
            "unknown": True
        }
        query = """ SELECT mesh_name AS ampname, mesh_longname AS longname,
                    mesh_description AS description, mesh_is_src AS is_src,
                    mesh_is_dst AS is_dst, mesh_active AS active
                    FROM mesh WHERE mesh_name = %s
                    """
        params = (mesh,)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying for mesh %s" % mesh)
            self.dblock.release()
            return None

        result = self.db.cursor.fetchone()
        if result is None:
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

        # flag the mesh with the appropriate test if it changed and is new
        self._flag_meshes_with_test(schedule_id)
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
        if self.db.executequery(query, params) == -1:
                log("Error while querying is_mesh()")
                self.dblock.release()
                return None

        count = self.db.cursor.fetchone()['count']
        self.db.closecursor()
        self.dblock.release()
        return count

    def _is_site(self, name):
        query = """ SELECT COUNT(*) FROM site WHERE site_ampname = %s """
        params = (name,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while querying is_site()")
                self.dblock.release()
                return None

        count = self.db.cursor.fetchone()['count']
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

    def _flag_mesh_as_source(self, mesh):
        query = """ UPDATE mesh SET mesh_is_src = true WHERE mesh_name = %s """
        params = (mesh,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating mesh")
                self.dblock.release()
                return None

        self.db.closecursor()
        self.dblock.release()
        return True

    def _flag_mesh_with_test(self, mesh, schedule_id):
        query = """ INSERT INTO meshtests (meshtests_name, meshtests_test)
                    SELECT %s,
                    CASE WHEN schedule_test='icmp' THEN 'latency'
                         WHEN schedule_test='tcpping' THEN 'latency'
                         WHEN schedule_test='dns' THEN 'latency'
                         WHEN schedule_test='traceroute' THEN 'hops'
                         ELSE schedule_test
                    END
                    FROM schedule WHERE schedule_id=%s
                    EXCEPT SELECT meshtests_name, meshtests_test FROM meshtests
                    WHERE meshtests_name=%s
                """
        params = (mesh, schedule_id, mesh)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating meshtests")
                self.dblock.release()
                return None

        self.db.closecursor()
        self.dblock.release()
        return True

    # TODO unflag meshes when the tests are removed?
    def _flag_meshes_with_test(self, schedule_id):
        # select all the meshes that are sources for this test
        query = """ SELECT endpoint_source_mesh, endpoint_destination_mesh
                    FROM endpoint
                    WHERE endpoint_schedule_id = %s """
        params = (schedule_id,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while fetching schedule meshes")
                self.dblock.release()
                return None

        meshes = []
        for row in self.db.cursor.fetchall():
            if row[0] is not None:
                meshes.append(row[0])
            if row[1] is not None:
                meshes.append(row[1])

        self.db.closecursor()
        self.dblock.release()

        for mesh in meshes:
            self._flag_mesh_with_test(mesh, schedule_id)

    def get_site_endpoints(self):
        query = """ SELECT DISTINCT endpoint_source_site AS ampname,
                    site_longname AS longname, site_location AS location,
                    site_description AS description
                    FROM endpoint JOIN site ON site_ampname=endpoint_source_site
                    WHERE endpoint_source_site IS NOT NULL """
        return self._sitequery(query, None)

    def add_endpoints_to_test(self, schedule_id, src, dst):
        # TODO avoid duplicate rows - set unique constraint?
        query = """ INSERT INTO endpoint (endpoint_schedule_id,
                    endpoint_source_mesh, endpoint_source_site,
                    endpoint_destination_mesh, endpoint_destination_site)
                    VALUES (%s, %s, %s, %s, %s) """
        if self._is_mesh(src):
            src_mesh = src
            src_site = None
            if self._flag_mesh_as_source(src) is None:
                return
            if self._flag_mesh_with_test(src, schedule_id) is None:
                return
        elif self._is_site(src):
            src_site = src
            src_mesh = None
            # TODO if a site in a mesh is the source of a test, should all
            # those meshes also be set as source meshes?
        else:
            print "source is neither mesh nor site"
            return

        if self._is_mesh(dst):
            dst_mesh = dst
            dst_site = None
            if self._flag_mesh_with_test(dst, schedule_id) is None:
                return
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
            # TODO set mesh_is_src=false if the mesh is no longer the source
            # of any tests
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
            print "destination is neither mesh nor site"
            return

        params = (schedule_id, src, dst)
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
                    string_agg(endpoint_destination_mesh, ','
                        ORDER BY endpoint_destination_mesh) AS dest_mesh,
                    string_agg(endpoint_destination_site, ','
                        ORDER BY endpoint_destination_site) AS dest_site
                    FROM endpoint JOIN schedule
                    ON schedule_id=endpoint_schedule_id
                    WHERE %s GROUP BY schedule_id
                    ORDER BY schedule_test, schedule_frequency, schedule_start
                """ % where

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

    # TODO if we want to be able to update the ampname then we probably need
    # to add an id field that won't change
    def update_mesh(self, ampname, longname, description):
        query = """ UPDATE mesh SET mesh_longname=%s, mesh_description=%s
                    WHERE mesh_name=%s """
        params = (longname, description, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def add_mesh(self, ampname, longname, description):
        # XXX currently all new meshes are created as destinations
        query = """ INSERT INTO mesh (mesh_name, mesh_longname,
                        mesh_description, mesh_is_src, mesh_is_dst, mesh_active
                    ) VALUES (%s, %s, %s, false, true, true) """
        params = (ampname, longname, description)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def delete_mesh(self, ampname):
        query = """ DELETE FROM mesh WHERE mesh_name=%s """
        params = (ampname, )

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def update_site(self, ampname, longname, location, description):
        query = """ UPDATE site SET site_longname=%s, site_location=%s,
                    site_description=%s WHERE site_ampname=%s """
        params = (longname, location, description, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def add_site(self, ampname, longname, location, description):
        # XXX currently all new meshes are created as destinations
        query = """ INSERT INTO site (site_ampname, site_longname,
                        site_location, site_description, site_active
                    ) VALUES (%s, %s, %s, %s, true) """
        params = (ampname, longname, location, description)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def delete_site(self, ampname):
        query = """ DELETE FROM site WHERE site_ampname=%s """
        params = (ampname, )

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def add_mesh_member(self, meshname, ampname):
        if self._is_mesh(ampname):
            return
        elif not self._is_site(ampname):
            # assume the destination is a site that was entered with the
            # text input box, and create it if it doesn't exist
            if self._add_basic_site(ampname) is None:
                return

        query = """ INSERT INTO member (member_meshname, member_ampname)
                    VALUES (%s, %s) """
        params = (meshname, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def delete_mesh_member(self, meshname, ampname):
        query = """ DELETE FROM member
                    WHERE member_meshname=%s AND member_ampname=%s """
        params = (meshname, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
                log("Error while updating test")
                self.dblock.release()
                return None
        self.db.closecursor()
        self.dblock.release()
        return True

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
