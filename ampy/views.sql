CREATE TABLE views (
    /* id number for external reference to this view */
    view_id SERIAL PRIMARY KEY,
    /* purely a label, no meaning to the program */
    view_label TEXT NOT NULL,
    /* list of groups that make up this view */
    view_groups integer[] /* ideally a foreign key into groups table */
);

CREATE TABLE groups (
    /* id number for external reference to this group by views table */
    group_id SERIAL PRIMARY KEY,
    /* textual description describing what to show in this view */
    group_description TEXT NOT NULL
);


/*
 * <test> FROM <src> TO <dst> OPTION <options> <split>
 *
 * Possible levels of aggregation:
 *  FULL - don't split streams
 *  NONE - return each stream individually
 *  FAMILY - split streams by address family
 *  NETWORK - split streams by /24 or /48
 */

/* Sample data */
INSERT INTO groups (group_description) VALUES
        ('amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.wand.net.nz OPTION 84 FULL'),
        ('amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.nzherald.co.nz OPTION 84 FULL');
INSERT INTO views (view_label, view_groups) VALUES ('dell to wand all', '{1}');
INSERT INTO views (view_label, view_groups) VALUES ('dell to nzh all', '{2}');
INSERT INTO views (view_label, view_groups) VALUES ('dell to wand,nzh all', '{1,2}');




