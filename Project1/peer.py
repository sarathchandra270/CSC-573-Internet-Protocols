# Peer implementation

import socket
import os
from struct import pack
from threading import Thread
import time
import sys
from os import listdir
import signal


# global variables
ERROR = 1
INFO = 2
DBG = 3
loglvl = INFO
stop_server = 0
RFCIndex = [] # initialize with random values
RfcServerPort = 65400 + int(sys.argv[1])
fl = open(str(RfcServerPort)+".log", "wb")
requiredRfc = []
peerName = "P"
RS_ip = socket.gethostname() # change to RS Host Name before running
#RS_ip = "engr-ras-203.eos.ncsu.edu"
RS_port = 65423
MAX_CLIENTS = 6
HDR_SIZE = 100
cookie = -1
demo = ""
activePeerList = []
timeRFC = {}
timePeer = {}

def log(lvl, msg):
    global RfcServerPort
    global fl
    if lvl <= loglvl:
        m = peerName + ": " +time.strftime('%H:%M:%S')+": "+ str(msg)+"\n"
        print m
        fl.write(m)

def composeRSMessage(msgType):
    global cookie
    global RfcServerPort
    msg = ""
    msgBody = ""
    hdr = msgType+ " P2P-DI/1.0\n"
    hdr += "Host: "+str(socket.gethostname())+":"+str(RfcServerPort)+"\nOS: "+str(sys.platform)+"\n"+"Content-Length: "
    if msgType == "Register":
   #     msgBody = msgType + "\n" + str(cookie) + "\n" + str(socket.gethostname()) + "\n" + str(RfcServerPort)
        msgBody = str(cookie)+"\n"+str(socket.gethostname()) + "\n" + str(RfcServerPort)
         
    elif msgType == "Leave" or msgType == "KeepAlive" or msgType == "PQuery":
        msgBody =  str(cookie)
    hdr += str(len(msgBody))+"\n"
    if len(hdr) < HDR_SIZE:
        for i in range(HDR_SIZE-len(hdr)-1):
            hdr+=" " 
        hdr+="\n"
    else:
        if len(hdr) > HDR_SIZE:
            log(DBG,"exceeded hdr_size by:"+str(len(hdr)-HDR_SIZE))
    msg = hdr+msgBody
    log(INFO, "Sending ====>\n"+msg)
    return msg

def parseRSMsgHdr(hdr):
    log(INFO, "Received <=====\n"+hdr)
    hdr = hdr.splitlines()
    i = hdr[3].split(":")
    return int(i[1])

def composePeerMessage(msgType):
    hdr = ""
    hdr = msgType+ " P2P-DI/1.0\n"
    hdr += "Host: "+str(socket.gethostname())+":"+str(RfcServerPort)+"\nOS: "+str(sys.platform)+"\n"+"Content-Length: 0\n"
    if len(hdr) < HDR_SIZE:
        for i in range(HDR_SIZE-len(hdr)-1):
            hdr+=" "
        hdr+="\n"
    else:
        if len(hdr) > HDR_SIZE:
            log(DBG,"exceeded hdr_size by:"+str(len(hdr)-HDR_SIZE))
    log(INFO,"Sending =====>\n"+hdr)
    return hdr

def composePeerMsgResp(msgType, size):
    hdr = ""
    hdr = msgType+ " P2P-DI/1.0\n"
    hdr += "Host: "+str(socket.gethostname())+":"+str(RfcServerPort)+"\nOS: "+str(sys.platform)+"\n"+"Content-Length: "+str(size)+"\n"
    if len(hdr) < HDR_SIZE:
        for i in range(HDR_SIZE-len(hdr)-1):
            hdr+=" "
        hdr+="\n"
    else:
        if len(hdr) > HDR_SIZE:
            log(DBG,"exceeded hdr_size by:"+str(len(hdr)-HDR_SIZE))
    log(INFO,"Sending ====>\n"+hdr)
    return hdr

def parsePeerMsgHdr(hdr):
    msg = ""
    log(INFO, "Received <=====\n"+hdr)
    hdr = hdr.splitlines()
    #i = hdr[0].split(" ")
    #log(DBG,"Received "+str(i[0]))    
    i = hdr[3].split(":")
    return int(i[1])

def registerWithRs(ip, port):
    global cookie
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    sock.send(composeRSMessage("Register"))
    rs_response = sock.recv(HDR_SIZE)
    #log(INFO,  str(rs_response))
    size = parseRSMsgHdr(rs_response)
    if (size >0): 
        rs_response = sock.recv(size)
        cookie = int(rs_response)
        log(INFO, str(rs_response))
    else:
        log(INFO, "Error in register response")
    sock.close()

def leavePeer(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    sock.send(composeRSMessage("Leave"))
    rs_response = sock.recv(HDR_SIZE)
    size = parseRSMsgHdr(rs_response)
    rs_response = ""
    while size > 0:
        if (size > 1024):
            length = 1024
        else:
            length = size
        size = size - length
        msg = sock.recv(length)
	rs_response += msg
	if length != len(msg):
            size += length - len(msg)
    #log(INFO, "Received: \n" + str(rs_response))
    sock.close()


def keepPeerAlive(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    sock.send(composeRSMessage("KeepAlive"))
    rs_response = sock.recv(HDR_SIZE)
    size = parseRSMsgHdr(rs_response)
    rs_response = ""
    while size > 0:
        if (size > 1024):
            length = 1024
        else:
            length = size
        size = size - length
	msg = sock.recv(length)
        rs_response += msg
        if length != len(msg):
            size += length - len(msg)
    #log(INFO, "Received: \n" + str(rs_response))
    sock.close()


def getActivePeers(ip, port):
    global activePeerList
    activePeerList = []
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    sock.send(composeRSMessage("PQuery"))
    try:
        rs_response = sock.recv(HDR_SIZE)
    except Exception as m:
        sock.close()
	return 0
    #print "rs_response:"
    #print rs_response
    size = parseRSMsgHdr(rs_response)
    if (size <=0):
	sock.close()
        return 0
    rs_response = ""
    while size > 0:
        if (size > 1024):
            length = 1024
        else:
            length = size
        size = size - length
    	msg = sock.recv(length)
        rs_response += msg
        if length != len(msg):
            size += length - len(msg)
    rs_response = rs_response.splitlines()
    #rs_response = rs_response[1:]
    cnt = 0
    for i in rs_response:
        #print "split rs_response:"
        #print i
        i = i.split(":")
        if not (i[0] == socket.gethostname() and i[1] == str(RfcServerPort)):
            #print "appending to the list..."
            #print i[0], i[1]
            #print i[0], i[1]
	    if (i[0], i[1]) not in activePeerList:
            	activePeerList.append((i[0], i[1]))
            	cnt+=1
            #print activePeerList
    sock.close()
    return cnt
    #activePeerList = [i for i in activePeerList if (i[0] != str(socket.gethostname()) and i[1] != str(RfcServerPort))]
    #print "active peer list is :"
    #print activePeerList
    #log(DBG, activePeerList)

def mergeRfcIndex(r_idx, p):
    global RFCIndex

    log(DBG,"before merge: "+ str(len(RFCIndex)))
 
    for i in r_idx.splitlines():
        j = i.split(":")
        found = 0
        for k, row in enumerate(RFCIndex):
            host,port,rfc,ttl = row
            if ((rfc == j[2]) and (host == j[0]) and (port == int(j[1]))):
                RFCIndex[k] = (host, port,rfc,7200)
                #log(DBG, "updated rfcindex: "+str(rfc))
                found = 1
                break
        if (found == 0):
            idx = (j[0], int(j[1]), j[2], 7200)
            RFCIndex.append(idx)

    log(DBG, "after merge: "+ str(len(RFCIndex)))
#def isRfcPresent():

def initRFCIndex():
    global RFCIndex
    global requiredRfc
    requiredRfc = listdir("latest_rfc")
    requiredRfc = [i for i in requiredRfc if not i.startswith(".")]
    
    
    #print requiredRfc
    #requiredRfc = listdir("P0")
    rfc_file = listdir(peerName)
    rfc_file = [i for i in rfc_file if not i.startswith(".")]
    #print peerName
    #print rfc_file
    log(DBG, str(len(rfc_file)))
    #print rfc_file
    for i in rfc_file:
        if i in requiredRfc:
            #print requiredRfc[requiredRfc.index(i)]
            log(DBG, "deleting " + str(requiredRfc[requiredRfc.index(i)]))
            del requiredRfc[requiredRfc.index(i)]
        idx = (socket.gethostname(), RfcServerPort, (i.split("."))[0], 7200)
        RFCIndex.append(idx)
    rfc_count = len(requiredRfc)
    if rfc_count > 50:
        log(DBG, "count > 50 truncating req rfc..." + str(rfc_count))
        requiredRfc = requiredRfc[:50]
    log(DBG, str(len(requiredRfc)))
    #print RFCIndex


def getRfcIndex(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #print ip,port 
    try:
        sock.connect((ip, port))
    except Exception as m:
        log(ERROR, m)
        return ""
        #sys.exit()
    try:
        sock.send(composePeerMessage("GET RFC-Index"))
    except Exception as m:
        log(ERROR, "send failed")
        return ""
    try:
        r = sock.recv(HDR_SIZE)
    except Exception as m:
        log(ERROR, "recv failed")
        return ""
    if not r:
        log(ERROR, "Error: No msg rcvd")
        return ""
    size = parsePeerMsgHdr(r)
    msg = ""
    while size > 0:
        if (size > 1024):
            length = 1024
        else:
            length = size
        size = size - length
	m = sock.recv(length)
        msg += m
        if length != len(m):
            size += length - len(m)

    sock.close()
    #print "size of rfcIndex: "+str(len(msg))+"\n"
    return msg


def retrieveRfc(p):
    global requiredRfc
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #print p[0], p[1], p[2]
    try:
        sock.connect((p[0], int(p[1])))
    except Exception as m:
        sock.close()
        return 0
    sock.send(composePeerMessage("GET RFC "+ p[2]))
    r = sock.recv(HDR_SIZE)
    # print str(r)
    f = open(peerName+"/"+p[2]+".txt", "wb")
    #f.write(r)
    size = parsePeerMsgHdr(r)
    res = ""
    size1 = size
    #print p[2]+" size: "+str(size)
    cnt = 0
    while size > 0:
        if (size > 1024):
            length = 1024
        else:
            length = size
        cnt += length
        size = size - length
        msg = sock.recv(length)
	if length != len(msg):
	    size += length - len(msg)
	    #log(INFO, "recevied bytes < length:  "+str(p[2])+"cnt:"+str(cnt)+" size1:"+str(length) + " size:"+str(len(msg))+"\n")
        res += msg
        #print "msg len : "+str(len(msg))
    #print "got bytes"+str(len(res))
    #if len(res) != size1:
	#log(INFO, "error in retrieving "+str(p[2])+"cnt:"+str(cnt)+" size1:"+str(size1) + " size:"+str(len(res))+"\n")
    f.write(res)
    f.close()
    del requiredRfc[requiredRfc.index(p[2]+".txt")]
    return 1

def getRfc(peer, rfc_idx):
    for i in rfc_idx.splitlines():
        #print i
        j = i.split(":")
        #print requiredRfc
        if (j[2]+".txt") in requiredRfc:
            #print "getting the rfc..."
            # place the time.start() here
            start_time = time.time()
            log(DBG, "retrieving " + str(j[2]))
            if retrieveRfc(j) == 0:
                break
            stop_time = time.time()
            timeRFC[j[2]] = stop_time - start_time
            if (j[0],j[1]) in timePeer.keys():
                timePeer[(j[0],j[1])] += stop_time - start_time
            else:
                timePeer[(j[0],j[1])] = stop_time - start_time
            if demo == "demo":
                log(INFO, "demo: sleeping for 5 secs for other peer to exit...");
                time.sleep(5)

def composeRfc(rfc_num):
    f = open(peerName+"/"+str(rfc_num)+".txt", "r")
    msg = f.read()
    f.close()
    hdr = composePeerMsgResp("OK RFC "+str(rfc_num), len(msg))
    hdr += msg
    return hdr

def composeRfcIndex():
    msg = ""
    for key in RFCIndex:
        msg += str(key[0]) + ":" + str(key[1]) + ":" + str(key[2])+"\n"
    hdr = composePeerMsgResp("OK RFC-Index", len(msg))
    hdr += str(msg)
    return hdr

def sendLeaveMsg():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((RS_ip, RS_port))
    sock.send(composeRSMessage("Leave"))
    rs_response = sock.recv(HDR_SIZE)
    #log(INFO,  str(rs_response))
    size = parseRSMsgHdr(rs_response)
    if (size >0):
        rs_response = sock.recv(size)
        cookie = int(rs_response)
        log(INFO, str(rs_response))
    else:
        log(ERROR, "Error in Leave response")
    sock.close()

class handlePeerRequest(Thread):
    def __init__(self, peer, addr):
        Thread.__init__(self)
        self.peer = peer
        self.addr = addr
    def run(self):
        global RFCIndex
        global stop_server
        r = self.peer.recv(HDR_SIZE)
        size = parsePeerMsgHdr(r)
        while size > 0:
            if (size > 1024):
                length = 1024
            else:
                length = size
            size = size - length
	    msg = sock.recv(length)
	    r += msg
            if length != len(msg):
                size += length - len(msg)
        #print "recevied message..."
        r = r.splitlines()
        if "GET RFC-Index " in r[0]:
            #print "sending rfcIndex list"
            self.peer.send(composeRfcIndex())
        if "GET RFC " in r[0]:
            r = r[0].split(" ")
            self.peer.send(composeRfc(r[2]))
            if demo == "demo":
                stop_server = 1
                sendLeaveMsg()
                log(DBG, "Leave SUCCESS ...")
                self.peer.close()
                os._exit(1)

        self.peer.close()

# RFC Server Code

class RFCServer(Thread):
    def __init__(self, host, port):
        Thread.__init__(self)
        self.host = host
        self.port = port
    
    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        log(DBG, "socket created successfully")
        #s.bind(('', self.port))
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1) 
        s.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,pack("ii",0,0)) 
        s.bind(('', self.port))
        log(DBG, "RFC Server Listening on %s" %(self.port))
        
        s.listen(MAX_CLIENTS)

        while True:
            peer, addr = s.accept()
            log(DBG, "new connection to rfcserver")
            log(DBG, str((peer, addr))) 
            handler = handlePeerRequest(peer, addr)
            handler.start()
            if demo == "demo":
                time.sleep(1)
            if stop_server == 1:
                log(INFO, "Exiting the RFC Server for demo...")
                s.close()
                break

# RFC Client Code

class RFCClient(Thread):
    def __init__(self, host):
        Thread.__init__(self)
        
        self.host = host

    def run(self):
        global RS_ip
        global RS_port
        global activePeerList
        global cookie
        global stop_server
        # create a socket and connect to the RS
        
        #client_socket.connect((RS_ip, RS_port))

        # register with RS
        registerWithRs(RS_ip, RS_port)
        cnt = 0
        while True:
            if len(requiredRfc) == 0 or stop_server == 1:
                break
            cnt += 1
            log(DBG, "Calling get active peers " + str(cnt)+": "+str(len(requiredRfc)))
            if getActivePeers(RS_ip, RS_port) == 0:
                  log(INFO, "No Active Peers. Waiting for 2 sec")
                  time.sleep(2)
            else:
                for p in activePeerList:
                    log(DBG, "got active peers from "+p[0] + ", " + p[1])
                    rfc_idx = getRfcIndex(p[0], int(p[1]))
                    if not rfc_idx:
                        continue
                    mergeRfcIndex(rfc_idx, p)
                    getRfc(p, rfc_idx)
                    if len(requiredRfc) == 0:
                        f = open("./task/"+peerName + "_rfc_dtime.csv", "wb")
                        f.write("rfc,dtime\n")
                        for i in timeRFC.keys():
                            f.write(str(i)+","+str(timeRFC[i])+"\n")
                        f.close()
                        f = open("./task/"+peerName+"_cumulative_dtime.csv", "wb")
                        f.write("peer,dtime\n")
                        for i in timePeer.keys():
                            f.write(str(i)+","+str(timePeer[i])+"\n")
                        f.close()
                        break
                time.sleep(2)
        log(INFO, "Finished Downloading RFCs...")

def updateTTLForRFCIndex():
    global RFCIndex

    log(DBG,"UpdateTTL: ")

    cnt = 0
    for k, row in enumerate(RFCIndex):
        host,port,rfc,ttl = row
        if ((host != socket.gethostname()) or (port != RfcServerPort)):
            RFCIndex[k] = (host, port,rfc,ttl-10)
            cnt += 1
    log(DBG,"TTL Updated for : "+str(cnt)+" remote RFCIndexes")


class KeepAliveHdlr(Thread):
    def __init__(self, host):
        Thread.__init__(self)

        self.host = host

    def run(self):
        global RS_ip
        global RS_port
        global fl
        global stop_server
        
        cnt =0
        while True:
            time.sleep(10)
            cnt +=1
            log(DBG, "KeepAlive woke up");
            updateTTLForRFCIndex()
            keepPeerAlive(RS_ip, RS_port)
            fl.flush()
            if (cnt >= 2):
                sendLeaveMsg()
                log(INFO, "Done with the download..Exiting ")
                os._exit(1)


def main():
    
    global peerName
    global demo
    global fl 
    # get local machine name
    peerName = "P"+sys.argv[1]
    if len(sys.argv) == 3:
        demo = sys.argv[2]
    log(INFO, peerName+" Started...\n")
    host = socket.gethostname()

    # Instantiate the RFC Server
    initRFCIndex()
    log(DBG, "len of required rfc is " + str(len(requiredRfc)))
    rfc_server = RFCServer(host, RfcServerPort)
    rfc_server.start()
    
    # Instantiate the RFC Client
    rfc_client = RFCClient(host)
    rfc_client.start()

    # Instantiate the KeepAlive Thread
    ka = KeepAliveHdlr(host)
    ka.start()


if __name__ == "__main__":
    main()

