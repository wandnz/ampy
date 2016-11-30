CREATE TABLE site (
    site_ampname TEXT NOT NULL PRIMARY KEY,
    site_longname TEXT NOT NULL,
    site_location TEXT,
    site_description TEXT,
    site_active BOOLEAN DEFAULT true,
    site_last_schedule_update INTEGER DEFAULT 0
);

CREATE TABLE mesh (
    mesh_name TEXT NOT NULL PRIMARY KEY,
    mesh_longname TEXT NOT NULL,
    mesh_description TEXT,
    mesh_is_src BOOLEAN NOT NULL,
    mesh_is_dst BOOLEAN NOT NULL,
    mesh_active BOOLEAN DEFAULT true,
    mesh_public BOOLEAN DEFAULT false
);

CREATE TABLE member (
    member_meshname TEXT NOT NULL REFERENCES mesh(mesh_name) ON DELETE CASCADE,
    member_ampname TEXT NOT NULL REFERENCES site(site_ampname) ON DELETE CASCADE
);
CREATE INDEX index_member_meshname ON member(member_meshname);
CREATE INDEX index_member_ampname ON member(member_ampname);

ALTER TABLE member ADD CONSTRAINT uniq_membership UNIQUE (member_meshname, member_ampname);

CREATE TABLE meshtests (
   meshtests_name TEXT NOT NULL REFERENCES mesh(mesh_name) ON DELETE CASCADE,
   meshtests_test TEXT NOT NULL
);

ALTER TABLE meshtests ADD CONSTRAINT uniq_meshtest UNIQUE (meshtests_name, meshtests_test);

CREATE VIEW active_mesh_members AS SELECT
    member_meshname as meshname,
    member_ampname as ampname,
    mesh_is_src,
    mesh_is_dst,
    mesh_public as public
    FROM mesh, member, site
    WHERE mesh.mesh_name=member.member_meshname
    AND member.member_ampname=site.site_ampname
    AND mesh_active=true
    AND site_active=true;

CREATE TABLE schedule (
    schedule_id SERIAL PRIMARY KEY,
    schedule_test TEXT NOT NULL,
    schedule_frequency INTEGER NOT NULL,
    schedule_start INTEGER,
    schedule_end INTEGER,
    schedule_period INTEGER, /* or TEXT? */
    schedule_args TEXT,
    schedule_enabled BOOLEAN NOT NULL DEFAULT true,
    schedule_mesh_offset INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE endpoint (
    endpoint_schedule_id INTEGER NOT NULL REFERENCES schedule(schedule_id) ON DELETE CASCADE,
    endpoint_source_mesh TEXT REFERENCES mesh(mesh_name),
    endpoint_source_site TEXT REFERENCES site(site_ampname),
    endpoint_destination_mesh TEXT REFERENCES mesh(mesh_name),
    endpoint_destination_site TEXT /*REFERENCES site(site_ampname)*/
);

ALTER TABLE endpoint ADD CONSTRAINT valid_source CHECK (
        endpoint_source_mesh IS NOT NULL OR
        endpoint_source_site IS NOT NULL);

/*
 * Don't allow duplicate combinations of sources and destinations. Can't do
 * it with a normal unique constraint due to some of the fields being NULL.
 */
CREATE UNIQUE INDEX unique_endpoints on endpoint (
    endpoint_schedule_id,
    COALESCE(endpoint_source_mesh, '-1'),
    COALESCE(endpoint_source_site, '-1'),
    COALESCE(endpoint_destination_mesh, '-1'),
    COALESCE(endpoint_destination_site, '-1')
);

CREATE VIEW full_mesh_details AS SELECT
    mesh_name as meshname,
    mesh_longname,
    mesh_description,
    mesh_is_src,
    mesh_is_dst,
    mesh_active,
    meshtests_test,
    mesh_public
    FROM mesh, meshtests
    WHERE mesh.mesh_name = meshtests.meshtests_name
    AND mesh_active = true;

/* TODO ampweb package should probably grant these permissions */
GRANT ALL ON ALL TABLES IN SCHEMA public to "www-data";
GRANT ALL ON ALL SEQUENCES IN SCHEMA public to "www-data";
