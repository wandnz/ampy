CREATE TABLE site (
    site_ampname TEXT NOT NULL PRIMARY KEY,
    site_longname TEXT NOT NULL,
    site_location TEXT,
    site_description TEXT,
    site_active BOOLEAN DEFAULT true
);

CREATE TABLE mesh (
    mesh_name TEXT NOT NULL PRIMARY KEY,
    mesh_longname TEXT NOT NULL,
    mesh_description TEXT,
    mesh_is_src BOOLEAN NOT NULL,
    mesh_is_dst BOOLEAN NOT NULL,
    mesh_active BOOLEAN DEFAULT true
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
    mesh_is_dst
    FROM mesh, member, site
    WHERE mesh.mesh_name=member.member_meshname
    AND member.member_ampname=site.site_ampname
    AND mesh_active=true
    AND site_active=true;

CREATE VIEW full_mesh_details AS SELECT
    mesh_name as meshname,
    mesh_longname,
    mesh_description,
    mesh_is_src,
    mesh_is_dst,
    mesh_active,
    meshtests_test
    FROM mesh, meshtests
    WHERE mesh.mesh_name = meshtests.meshtests_name
    AND mesh_active = true;

/* TODO ampweb package should probably grant these permissions */
GRANT ALL ON ALL TABLES IN SCHEMA public to "www-data";
