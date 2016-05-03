import psycopg2
import psycopg2.extras
import argparse, sys, string

uncommitted = 0

def insert_asn(db, asn, name):
    global uncommitted

    query = "INSERT into asmap VALUES (%s, %s)"
    params = (asn, name)

    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.DatabaseError as e:
        print "Failed to create cursor", e
        return None

    try:
        cursor.execute(query, params)
    except psycopg2.IntegrityError as e:
        #print "Attempted to insert duplicate ASN %s" % (params[0])
        db.rollback()
        return None
    except psycopg2.Error as e:
        print "Failed to insert site", e
        return None

    uncommitted += 1
    if (uncommitted >= 1000):
        db.commit()
        uncommitted = 0
    cursor.close()
    return True


# sourcefile is assumed to be a textual dump of
# http://www.cidr-report.org/as2.0/autnums.html

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--sourcefile", help="Specify the file containing the list of ASNs and their names", default="")
    parser.add_argument("-d", "--database", help="Specify the database to insert the ASNs into", default="amp-asmap")

    args = parser.parse_args()
    if args.sourcefile == "":
        print "Please specify a source file using -f!"
        sys.exit(1)

    connstr = "dbname=%s" % (args.database)

    try:
        dbconn = psycopg2.connect(connstr)
    except psycopg2.DatabaseError as e:
        print e
        sys.exit(1)

    f = open(args.sourcefile)

    for line in f:
        if line == "" or len(line) < 3:
            continue
        if not line.startswith("AS"):
            continue

        asn = line.split()[0][2:]

        # Remove first and last word -- first is the ASN, last is the country
        name = " ".join(line.split()[1:])
        
        if (insert_asn(dbconn, asn, name) is None):
            continue
    dbconn.commit()
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
