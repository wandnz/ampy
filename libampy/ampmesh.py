import time
from threading import Lock
from libampy.database import AmpyDatabase
from libnntscclient.logger import *

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
    get_endpoints_by_name:
        Returns a list of sources or destinations that contain a given
        substring in their AMP name
    """

    # XXX seriously, make the schedule stuff its own class.
    # XXX this needs to be accessible by ampweb
    SCHEDULE_OPTIONS = ["test", "source", "destination", "frequency", "start",
                        "end", "period", "mesh_offset", "args"]

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

    def _meshquery(self, query, params, lock=True):
        """
        Performs a basic query for mesh members and returns a list of results.

        Parameters:

          query -- the query to perform, as a parameterised string
          params -- a tuple containing parameters to substitute into the query

        Returns:
          a list of results returned by the query
        """
        sites = []

        if lock:
            self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying mesh members")
            self.dblock.release()
            return None

        for row in self.db.cursor.fetchall():
            sites.append(row['ampname'])
        self.db.closecursor()
        if lock:
            self.dblock.release()
        return sites


    def get_mesh_sources(self, mesh, lock=True):
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
        return self._meshquery(query, params, lock)

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

    def get_meshes(self, endpoint, amptest=None, site=None, public=None):
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
            statustable = "active_mesh_members"
            table = """ active_mesh_members JOIN full_mesh_details
                ON active_mesh_members.meshname = full_mesh_details.meshname """
        else:
            # otherwise use all possible meshes, even if they have no tests
            meshname = "mesh_name"
            statustable = "mesh"
            table = """ active_mesh_members RIGHT JOIN mesh
                ON active_mesh_members.meshname = mesh.mesh_name """

        # XXX count isnt always sensible? if WHERE is involved
        query = """ SELECT %s as mesh_name, mesh_longname,
                    mesh_description, count(active_mesh_members.*), mesh_public
                    FROM %s WHERE mesh_active = true """ % (meshname, table)

        # if site is set then only return meshes that it belongs to
        if site is not None:
            query += " AND ampname = %s "
            params.append(site)

        # if public is set then filter meshes by the appropriate value
        if public is not None:
            query += " AND public = %s "
            params.append(public)

        # if test is set then only return destination meshes that the test is
        # performed to (ignored for source meshes)
        if amptest is not None and endpoint == "destination":
            query += " AND meshtests_test = %s "
            params.append(amptest)

        # return meshes of the appropriate type - source or dest
        if endpoint == "source":
            query += " AND %s.mesh_is_src = true " % statustable
        elif endpoint == "destination":
            query += " AND %s.mesh_is_dst = true" % statustable
        else:
            # for now just return source and destination meshes if no endpoint
            # is set, though this doesn't play that well with amptest being set
            pass

        query += " GROUP BY mesh_name, mesh_longname, mesh_description, mesh_public ORDER BY mesh_longname"

        self.dblock.acquire()
        if self.db.executequery(query, tuple(params)) == -1:
            log("Error while querying %s meshes" % (endpoint))
            self.dblock.release()
            return None

        meshes = []
        for row in self.db.cursor.fetchall():
            meshes.append({'ampname':row[0], 'longname':row[1], \
                    'description':row[2], 'count':row[3], 'public':row[4]})

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

    def get_sites(self):
        query = """ SELECT site_ampname AS ampname, site_longname AS longname,
                    site_location AS location, site_description AS description
                    FROM site ORDER BY longname """
        return self._sitequery(query, None)

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

    def get_endpoints_by_name(self, issrc=True, pagesize=30, offset=0, term=""):
        """
        Fetches known endpoints that contain a given substring in their AMP
        name.

        Parameters:
          issrc: if True, only return endpoints that are AMP test sources,
                 otherwise only return endpoints that are AMP test targets.
          pagesize: the maximum number of sources to return.
          offset: the number of sources to skip, starting from the front of
                  the source list.
          term: the substring that must be present in each returned source's
                AMP name.

        Returns:
          a two tuple -- the first element is the total number of sources that
          contained the given substring, the second element is a list of
          matching sources given the offset and pagesize constraints.
        """

        # TODO: mesh-less sites are ignored by this -- sites need to record
        # whether they can be a source or not, rather than meshes.
        # When this changes, update these queries to reflect that.

        if issrc:
            countquery = """ SELECT DISTINCT site_ampname AS ampname
                    FROM site JOIN active_mesh_members ON
                    site.site_ampname = active_mesh_members.ampname WHERE
                    mesh_is_src = true AND site.site_ampname ILIKE %s """
            epquery = """ SELECT DISTINCT site_ampname AS ampname
                    FROM site JOIN active_mesh_members ON
                    site.site_ampname = active_mesh_members.ampname WHERE
                    mesh_is_src = true AND site.site_ampname ILIKE %s
                    ORDER BY ampname LIMIT %s OFFSET %s  """
        else:
            countquery = """ SELECT DISTINCT site_ampname AS ampname
                    FROM site JOIN active_mesh_members ON
                    site.site_ampname = active_mesh_members.ampname WHERE
                    mesh_is_dst = true AND site.site_ampname ILIKE %s """
            epquery = """ SELECT DISTINCT site_ampname AS ampname
                    FROM site JOIN active_mesh_members ON
                    site.site_ampname = active_mesh_members.ampname WHERE
                    mesh_is_dst = true AND site.site_ampname ILIKE %s
                    ORDER BY ampname LIMIT %s OFFSET %s  """


        matched = []
        params = ("%" + term + "%",)

        self.dblock.acquire()
        if self.db.executequery(countquery, params) == -1:
            log("Error while querying for site counts")
            self.dblock.release()
            return 0, []

        epcount = self.db.cursor.rowcount
        self.db.closecursor()

        params = ("%" + term + "%", pagesize, offset)

        if self.db.executequery(epquery, params) == -1:
            log("Error while querying for sites")
            self.dblock.release()
            return 0, []

        for row in self.db.cursor:
            matched.append({'id': row[0], 'text': row[0]})
            if len(matched) > pagesize:
                break

        self.db.closecursor()
        self.dblock.release()
        return epcount, matched


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
            "last_schedule_update": 0,
            "unknown": True
        }

        query = """ SELECT site_ampname as ampname, site_longname as longname,
                    site_location as location, site_description as description,
                    site_active as active,
                    site_last_schedule_update AS last_schedule_update
                    FROM site WHERE site_ampname = %s
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
            "public": False,
            "unknown": True
        }
        query = """ SELECT mesh_name AS ampname, mesh_longname AS longname,
                    mesh_description AS description, mesh_is_src AS is_src,
                    mesh_is_dst AS is_dst, mesh_active AS active,
                    mesh_public AS public
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
    def schedule_new_test(self, settings):
        query = """ INSERT INTO schedule (schedule_test, schedule_frequency,
                    schedule_start, schedule_end, schedule_period,
                    schedule_args, schedule_mesh_offset)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING schedule_id """

        # TODO sanity check arguments? make sure test exists etc
        try:
            params = (
                settings["test"], settings["frequency"], settings["start"],
                settings["end"], settings["period"], settings["args"],
                settings["mesh_offset"]
            )
        except KeyError:
            return None

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while scheduling new test")
            self.dblock.release()
            return None

        schedule_id = self.db.cursor.fetchone()['schedule_id']
        self.db.closecursor()
        self.dblock.release()

        # add the initial set of endpoints for this test
        self.add_endpoints_to_test(schedule_id, settings["source"],
                settings["destination"])

        return schedule_id


    def update_test(self, schedule_id, settings):
        changes = []
        params = []

        for option in self.SCHEDULE_OPTIONS:
            if option in settings:
                changes.append("schedule_%s=%%s" % option)
                params.append(settings[option])

        # no valid options were set, do nothing and report that we did it ok
        if len(changes) < 1:
            return True

        query = "UPDATE schedule SET "+",".join(changes)+" WHERE schedule_id=%s"
        params.append(schedule_id)

        self.dblock.acquire()
        if self.db.executequery(query, tuple(params)) == -1:
            log("Error while updating test")
            self.dblock.release()
            return None
        count = self.db.cursor.rowcount
        self.db.closecursor()
        if count > 0:
            self._update_last_modified_schedule(schedule_id)
        self.dblock.release()

        # flag the mesh with the appropriate test if it changed and is new
        if count > 0 and "test" in settings:
            self._flag_meshes_with_test(schedule_id)
        return count > 0

    def get_enable_status(self, schedule_id):
        query = "SELECT schedule_enabled FROM schedule WHERE schedule_id=%s"
        params = (schedule_id,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying status of scheduled test")
            self.dblock.release()
            return None

        result = self.db.cursor.fetchone()
        if result is None:
            self.db.closecursor()
            self.dblock.release()
            # XXX this should be false but then that gets confused with disabled
            return None

        self.db.closecursor()
        self.dblock.release()
        return result[0]

    def enable_disable_test(self, schedule_id, enabled):
        query = "UPDATE schedule SET schedule_enabled=%s WHERE schedule_id=%s"
        params = (enabled, schedule_id)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while changing status of scheduled test")
            self.dblock.release()
            return None
        count = self.db.cursor.rowcount
        self.db.closecursor()
        if count > 0:
            self._update_last_modified_schedule(schedule_id)
        self.dblock.release()
        return count > 0

    def delete_test(self, schedule_id):
        query = """ DELETE FROM schedule WHERE schedule_id=%s """
        params = (schedule_id,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while deleting scheduled test")
            self.dblock.release()
            return None

        count = self.db.cursor.rowcount
        self.db.closecursor()
        self.dblock.release()
        return count > 0

    def _is_mesh(self, name, lock=True):
        query = """ SELECT COUNT(*) FROM mesh WHERE mesh_name = %s """
        params = (name,)
        if lock:
            self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying is_mesh()")
            self.dblock.release()
            return None

        count = self.db.cursor.fetchone()['count']
        self.db.closecursor()
        if lock:
            self.dblock.release()
        return count

    def _is_site(self, name, lock=True):
        query = """ SELECT COUNT(*) FROM site WHERE site_ampname = %s """
        # remove any suffixes from the name, e.g. !v4 !v6 family specifiers
        params = (name.split('!', 1)[0],)
        if lock:
            self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying is_site()")
            self.dblock.release()
            return None

        count = self.db.cursor.fetchone()['count']
        self.db.closecursor()
        if lock:
            self.dblock.release()
        return count

    def _add_basic_site(self, name):
        query = """ INSERT INTO site (site_ampname, site_longname)
                    VALUES (%s, %s) """
        # remove any suffixes from the name, e.g. !v4 !v6 family specifiers
        params = (name.split('!', 1)[0], name.split('!', 1)[0])
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
                         WHEN schedule_test='throughput' THEN 'tput'
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
        # select all the meshes that are involved in this test
        query = """ SELECT endpoint_source_mesh,
                      endpoint_destination_mesh
                    FROM endpoint
                    WHERE endpoint_schedule_id = %s """
        params = (schedule_id,)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while fetching schedule meshes")
            self.dblock.release()
            return None

        meshes = set()
        for row in self.db.cursor.fetchall():
            if row[0] is not None:
                meshes.add(row[0])
            if row[1] is not None:
                meshes.add(row[1])

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
        if self._is_mesh(src):
            src_mesh = src
            src_site = None
            if self._flag_mesh_as_source(src) is None:
                return None
            if self._flag_mesh_with_test(src, schedule_id) is None:
                return None
        elif self._is_site(src):
            src_site = src
            src_mesh = None
            # TODO if a site in a mesh is the source of a test, should all
            # those meshes also be set as source meshes?
        else:
            print "source is neither mesh nor site"
            return None

        if dst is None:
            dst_site = None
            dst_mesh = None
        elif self._is_mesh(dst):
            dst_mesh = dst
            dst_site = None
            if self._flag_mesh_with_test(dst, schedule_id) is None:
                return None
        elif self._is_site(dst):
            dst_site = dst
            dst_mesh = None
        else:
            # assume the destination is a site that was entered with the
            # text input box, and create it if it doesn't exist
            dst_site = dst
            dst_mesh = None
            if self._add_basic_site(dst) is None:
                return None

        params = [schedule_id, src_mesh, src_site, dst_mesh, dst_site]
        subquery = "SELECT 1 FROM endpoint WHERE endpoint_schedule_id=%s"
        params.append(schedule_id)

        if src_mesh:
            subquery += " AND endpoint_source_mesh=%s"
            params.append(src_mesh)
        else:
            subquery += " AND endpoint_source_mesh is NULL"

        if src_site:
            subquery += " AND endpoint_source_site=%s"
            params.append(src_site)
        else:
            subquery += " AND endpoint_source_site is NULL"

        if dst_mesh:
            subquery += " AND endpoint_destination_mesh=%s"
            params.append(dst_mesh)
        else:
            subquery += " AND endpoint_destination_mesh is NULL"

        if dst_site:
            subquery += " AND endpoint_destination_site=%s"
            params.append(dst_site)
        else:
            subquery += " AND endpoint_destination_site is NULL"

        query = """ INSERT INTO endpoint (endpoint_schedule_id,
                    endpoint_source_mesh, endpoint_source_site,
                    endpoint_destination_mesh, endpoint_destination_site)
                    SELECT %%s, %%s, %%s, %%s, %%s WHERE NOT EXISTS (%s) """ % (
                    subquery)

        self.dblock.acquire()
        if self.db.executequery(query, tuple(params)) == -1:
            log("Error while inserting new test endpoints")
            self.dblock.release()
            return None
        count = self.db.cursor.rowcount
        self.db.closecursor()
        if count > 0:
            self._update_last_modified_schedule(schedule_id)
        self.dblock.release()
        return count > 0

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
            return None

        if self._is_mesh(dst):
            query += " AND endpoint_destination_mesh=%s"
        elif self._is_site(dst):
            query += " AND endpoint_destination_site=%s"
        else:
            print "destination is neither mesh nor site"
            return None

        params = (schedule_id, src, dst)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while deleting endpoints")
            self.dblock.release()
            return None
        count = self.db.cursor.rowcount
        self.db.closecursor()
        if count > 0:
            self._update_last_modified_schedule(schedule_id)
        self.dblock.release()
        return count > 0

    # TODO
    # XXX expects the db lock to be held by caller, so we can update
    # this at the same time as the modifications get made
    # XXX do something useful with return values so we don't carry on after
    # a broken update
    def _update_last_modified_schedule(self, schedule_id):
        # get all sites involved with this test item and update them
        sites = []
        schedule = self._get_endpoint_schedule(None, None, schedule_id, False)
        for item in schedule:
            sites += item["source_site"]
            for mesh in item["source_mesh"]:
                sites += self.get_mesh_sources(mesh, lock=False)
        for site in set(sites):
            self._update_last_modified_site(site)
        return True

    # XXX expects the db lock to be held by caller, so we can update
    # this at the same time as the modifications get made
    # XXX do something useful with return values so we don't carry on after
    # a broken update
    def _update_last_modified_site(self, ampname):
        # update last modified time of schedule
        query = """ UPDATE site
                    SET site_last_schedule_update=%s WHERE site_ampname=%s"""
        params = (int(time.time()), ampname)

        # XXX can we ROLLBACK if this failed? or does autocommit screw us?
        if self.db.executequery(query, params) == -1:
            log("Error updating schedule modification time")
            return None

        self.db.closecursor()
        return True

    def get_source_schedule(self, source, schedule_id=None, lock=True):
        return self._get_endpoint_schedule(source, None, schedule_id, lock)

    def get_destination_schedule(self, destination, schedule_id=None,lock=True):
        return self._get_endpoint_schedule(None, destination, schedule_id, lock)

    def get_schedule_by_id(self, schedule_id, lock=True):
        schedule = self._get_endpoint_schedule(None, None, schedule_id, lock)
        if schedule is None:
            return None
        if len(schedule) == 0:
            return {}
        return schedule[0]

    def _get_endpoint_schedule(self, src, dst, schedule_id, lock):
        """
        Fetch all scheduled tests that involve the given endpoints

        Parameters:
          src -- the name of the source site/mesh to fetch the schedule for
          dst -- the name of the destination site/mesh to fetch the schedule for

        Returns:
          a list containing the scheduled tests to these endpoints
        """

        if src is None and dst is None and schedule_id is None:
            return None

        where = []
        params = []

        if src is not None:
            if self._is_mesh(src, lock):
                where.append("endpoint_source_mesh=%s")
            elif self._is_site(src, lock):
                where.append("endpoint_source_site=%s")
            else:
                return None
            params.append(src)

        if dst is not None:
            if self._is_mesh(dst, lock):
                where.append("endpoint_destination_mesh=%s")
            elif self._is_site(dst, lock):
                where.append("endpoint_destination_site=%s")
            else:
                return None
            params.append(dst)

        if schedule_id is not None:
            where.append("schedule_id=%s")
            params.append(schedule_id)

        query = """ SELECT schedule_id, schedule_test, schedule_frequency,
                    schedule_start, schedule_end, schedule_period,
                    schedule_args,
                    string_agg(DISTINCT endpoint_source_mesh, ','
                        ORDER BY endpoint_source_mesh) AS source_mesh,
                    string_agg(DISTINCT endpoint_source_site, ','
                        ORDER BY endpoint_source_site) AS source_site,
                    string_agg(endpoint_destination_mesh, ','
                        ORDER BY endpoint_destination_mesh) AS dest_mesh,
                    string_agg(endpoint_destination_site, ','
                        ORDER BY endpoint_destination_site) AS dest_site,
                    schedule_enabled, schedule_mesh_offset
                    FROM endpoint JOIN schedule
                    ON schedule_id=endpoint_schedule_id
                    WHERE %s GROUP BY schedule_id
                    ORDER BY schedule_test, schedule_frequency, schedule_start
                """ % (" AND ".join(where))

        schedule = []

        if lock:
            self.dblock.acquire()

        if self.db.executequery(query, tuple(params)) == -1:
            log("Error while querying for schedule")
            self.dblock.release()
            return None

        for row in self.db.cursor.fetchall():
            source_meshes = [] if row[7] is None else row[7].split(",")
            dest_meshes = [] if row[9] is None else row[9].split(",")
            source_sites = [] if row[8] is None else row[8].split(",")
            dest_sites = [] if row[10] is None else row[10].split(",")
            schedule.append({'id':row[0], 'test':row[1], 'enabled':row[11],
                    'frequency':row[2], 'start':row[3], 'mesh_offset':row[12],
                    'end':row[4], 'period':row[5], 'args':row[6],
                    'source_mesh':source_meshes, 'source_site':source_sites,
                    'dest_mesh':dest_meshes, 'dest_site':dest_sites})

        self.db.closecursor()
        if lock:
            self.dblock.release()
        return schedule

    # TODO if we want to be able to update the ampname then we probably need
    # to add an id field that won't change
    def update_mesh(self, ampname, longname, description, public):
        query = """ UPDATE mesh SET mesh_longname=%s, mesh_description=%s,
                    mesh_public=%s
                    WHERE mesh_name=%s """
        params = (longname, description, public, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while updating mesh")
            self.dblock.release()
            return None
        self.db.closecursor()
        self.dblock.release()
        return True

    def add_mesh(self, ampname, longname, description, public):
        # XXX currently all new meshes are created as destinations
        query = """ INSERT INTO mesh (mesh_name, mesh_longname,
                        mesh_description, mesh_is_src, mesh_is_dst,
                        mesh_active, mesh_public
                    ) VALUES (%s, %s, %s, false, true, true, %s) """
        params = (ampname, longname, description, public)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while adding mesh")
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
            log("Error while deleting mesh")
            self.dblock.release()
            return None
        count = self.db.cursor.rowcount
        self.db.closecursor()
        self.dblock.release()
        return count > 0

    def update_site(self, ampname, longname, location, description):
        query = """ UPDATE site SET site_longname=%s, site_location=%s,
                    site_description=%s WHERE site_ampname=%s """
        params = (longname, location, description, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while updating site")
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
            log("Error while adding site")
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
            log("Error while deleting site")
            self.dblock.release()
            return None
        count = self.db.cursor.rowcount
        self.db.closecursor()
        self.dblock.release()
        return count > 0

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
            log("Error while adding mesh member")
            self.dblock.release()
            return None
        self.db.closecursor()

        # update the site so it will fetch tests belonging to the new mesh
        self._update_last_modified_site(ampname)

        # update all sites that test to this mesh to include this target
        for schedule in self.get_destination_schedule(meshname, lock=False):
            self._update_last_modified_schedule(schedule["id"])

        self.dblock.release()

        return True

    def delete_mesh_member(self, meshname, ampname):
        query = """ DELETE FROM member
                    WHERE member_meshname=%s AND member_ampname=%s """
        params = (meshname, ampname)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while deleting mesh member")
            self.dblock.release()
            return None
        self.db.closecursor()

        # update the site so it will remove tests belonging to the new mesh
        self._update_last_modified_site(ampname)

        # update all sites that test to this mesh to remove this target
        for schedule in self.get_destination_schedule(meshname, lock=False):
            self._update_last_modified_schedule(schedule["id"])

        self.dblock.release()

        return True

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
