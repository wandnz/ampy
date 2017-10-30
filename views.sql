CREATE TABLE IF NOT EXISTS views (
    /* id number for external reference to this view */
    view_id SERIAL PRIMARY KEY,
    /* the collection that this view belongs to */
    collection TEXT NOT NULL,
    /* list of groups that make up this view */
    view_groups integer[] /* ideally a foreign key into groups table */
);

CREATE TABLE IF NOT EXISTS groups (
    /* id number for external reference to this group by views table */
    group_id SERIAL PRIMARY KEY,
    /* the collection that this group belongs to */
    collection TEXT NOT NULL,
    /* textual description describing what to show in this view */
    group_description TEXT NOT NULL
);

CREATE INDEX idx_group_collection ON groups (collection);
CREATE INDEX idx_view_collection ON views (collection);

);

/* TODO ampweb package should probably grant these permissions */
GRANT ALL ON ALL TABLES IN SCHEMA public to "ampweb";
GRANT ALL ON ALL SEQUENCES IN SCHEMA public to "ampweb";
