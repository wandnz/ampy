from libnntscclient.logger import *
import socket

def queryASNames(toquery, localcache=None):
    if len(toquery) == 0:
        return {}

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)

    try:
        s.connect(('whois.cymru.com', 43))
    except socket.error, msg:
        log("Failed to connect to whois.cymru.com:43, %s" % (msg))
        s.close()
        return {}

    msg = "begin\n"
    for q in toquery:
        msg += q + "\n"
    msg += "end\n"

    totalsent = 0
    while totalsent < len(msg):
        sent = s.send(msg[totalsent:])
        if sent == 0:
            log("Error while sending query to whois.cymru.com")
            s.close()
            return {}
        totalsent += sent

    # Receive all our responses
    responded = 0
    recvbuf = ""
    asnames = {}

    inds = list(toquery)
    while responded < len(toquery):
        chunk = s.recv(2048)
        if chunk == '':
            break
        recvbuf += chunk

        if '\n' not in recvbuf:
            continue

        lines = recvbuf.splitlines(True)
        consumed = 0
        for l in lines:
            if l[-1] == "\n":
                if "Bulk mode" not in l:
                    asnames[inds[responded]] = l.strip()
                    if localcache:
                        localcache.store_asname(inds[responded], l.strip())
                    responded += 1
                consumed += len(l)
        recvbuf = recvbuf[consumed:]
    s.close()

    return asnames

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
