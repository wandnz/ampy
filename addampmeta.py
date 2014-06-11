import psycopg2
import psycopg2.extras
import argparse, sys

def insert_mesh(db, params):
    assert(len(params) == 5)

    for i in range(3, 5):
        if params[i] == "f" or params[i] == "F":
            params[i] = False
        elif params[i] == "t" or params[i] == "T":
            params[i] = True
        else:
            print "Mesh parameters 4 and 5 must be either 't' or 'f' -- skipping"
            return None

    query = "INSERT into mesh VALUES (%s, %s, %s, %s, %s, True)"
    paramstup = tuple(params)

    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.DatabaseError as e:
        print "Failed to create cursor", e
        return None

    try:
        cursor.execute(query, paramstup)
    except psycopg2.IntegrityError as e:
        print "Attempted to insert duplicate mesh %s" % (params[0])
    except psycopg2.Error as e:
        print "Failed to insert mesh", e
        return None

    db.commit()
    cursor.close()
    return True
        
def insert_site(db, params):
    assert(len(params) >= 5)

    query = "INSERT into site VALUES (%s, %s, %s, %s, True)"
    paramstup = tuple(params[0:4])

    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.DatabaseError as e:
        print "Failed to create cursor", e
        return None

    try:
        cursor.execute(query, paramstup)
    except psycopg2.IntegrityError as e:
        print "Attempted to insert duplicate site %s" % (params[0])
        db.rollback()
    except psycopg2.Error as e:
        print "Failed to insert site", e
        return None

    meshes = params[4:]

    for m in meshes:
        query = "INSERT into member VALUES (%s, %s)"
        paramstup = (m, params[0])

        try:
            cursor.execute(query, paramstup)
        except psycopg2.IntegrityError as e:
            print "Attempted to insert duplicate mesh membership %s %s" % paramstup
            db.rollback()
        except psycopg2.Error as e:
            print "Failed to insert mesh membership", e
            return None


    db.commit()
    cursor.close()
    return True
        



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--sourcefile", help="Specify the file containing the sites and meshs you wish to add", default="")
    parser.add_argument("-d", "--database", help="Specify the database to insert the new sites into", default="ampmeta")

    args = parser.parse_args()
    if args.sourcefile == "":
        print "Please specify a source file using -f!"
        sys.exit(1)

    # TODO Add database options?
    connstr = "dbname=%s" % (args.database)

    try:
        dbconn = psycopg2.connect(connstr)
    except psycopg2.DatabaseError as e:
        print e
        sys.exit(1)

    f = open(args.sourcefile)
    for line in f:
        if line == "" or line[0] == '#':
            continue
        line = line.strip()
        params = [x.strip() for x in line.split('|')]

        if params[0] == "mesh":
            if insert_mesh(dbconn, params[1:]) == None:
                pass
        if params[0] == "site":
            if insert_site(dbconn, params[1:]) == None:
                pass





# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :




