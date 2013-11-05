CREATE TABLE views (
    /* purely a label, no meaning to the program */
    view_label TEXT NOT NULL,
    /* textual description describing what to show in this view */
    view_group TEXT NOT NULL,
);
/* TODO index on view_label */


/*
 * <test> FROM <src> TO <dst> <split>
 *
 * Possible splits:
 *  COMBINED - don't split streams
 *  ALL - return each stream individually
 *  FAMILY - split streams by address family
 *  NETWORK - split streams by /24 or /48
 */

 /* Sample data */
INSERT INTO views VALUES
    (
     'matrix_test_test',
     'amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.nzherald.co.nz COMBINED'
    ),
    (
     'matrix_test_test',
     'amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.wand.net.nz COMBINED'
    ),
    (
     'dell_to_wand_nzh_all',
     'amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.nzherald.co.nz ALL'
    ),
    (
     'dell_to_wand_nzh_all',
     'amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.wand.net.nz ALL'
    ),
    (
     'dell_to_nzh_network',
     'amp-icmp FROM waikato-dell.amp.wand.net.nz TO www.wand.net.nz NETWORK'
    ) ;
