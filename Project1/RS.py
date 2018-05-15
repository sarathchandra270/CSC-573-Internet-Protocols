# Registration Server Implementation

import socket
from threading import Thread, Lock
from struct import pack
import time
import sys

# global variables
ERR = 1
INFO = 2
DBG = 3
loglvl = INFO
HDR_SIZE = 100
MAX_CLIENTS = 5
PORT = 65423
peerList = {}
cookieCount = 1
mutex = Lock()
fl = open(str(PORT)+".log", "wb")

# structure that defines the peer

class PeerNode:
    def __init__(self):
        self.hostname = ""
        self.cookie = -1
        self.isActive = 0
        self.ttl = 7200
        self.port = -1
        self.activeCount = 0
        self.lastRegisteredTime = ""


def log(lvl, msg):
    if lvl <= loglvl:
        m = str(PORT) + ": " + time.strftime('%H:%M:%S')+": "+str(msg)
        print m
        fl.write(m)
        fl.flush()


# checks if a peer is already registered with the RS

def isPeerRegistered(addr):
    ip_addr = addr # change this to addr to be able to test the p2p system on local machine
    global peerList
    log(DBG,str( peerList.keys()))
    log(DBG, str(ip_addr))
    if ip_addr in peerList.keys():
        # print "ip address found!!"
        return True
    else:
        return False

def composeMsgResp(msgType, msg):
    hdr = ""
    hdr = msgType+ " P2P-DI/1.0\n"
    hdr += "Host: "+str(socket.gethostname())+":"+str(PORT)+"\nOS: "+str(sys.platform)+"\n"+"Content-Length: "+str(len(msg))+"\n"
    if len(hdr) < HDR_SIZE:
        for i in range(HDR_SIZE-len(hdr)-1):
            hdr+=" "
        hdr+="\n"
    else:
        if len(hdr) > HDR_SIZE:
            log(DBG,"exceeded hdr_size by:"+str(len(hdr)-HDR_SIZE))
    hdr+=msg
    log(INFO,"Sending ====>\n"+hdr)
    return hdr

def parseMsgHdr(hdr):
    log(INFO,"Received <====\n"+hdr)
    hdr = hdr.splitlines()
    i = hdr[0].split(" ")
    mType = i[0]
    i = hdr[3].split(":")
    return (int(i[1]), mType)

# registers a peer with the RS

def registerPeer(addr, peerMessage):
    node = PeerNode()
    global cookieCount
    global peerList
    node.hostname = peerMessage[1] # ip address of the peer that connected to the RS
    node.isActive = 1
    node.ttl = 7200
    node.port = peerMessage[2]
    node.activeCount = 1
    node.lastRegisteredTime = time.strftime('%H:%M:%S')
    peerList[cookieCount] = node
    #cookieCount += 1

# removes a peers registration with the RS

def deactivatePeer(addr):
    global peerList
    peerList[int(addr)].isActive = 0
    log(DBG, "Deactivated" + str(peerList[int(addr)].port))


# updates the time to live for a peer

def updatePeerTTL(addr):
    global peerList
    peerList[int(addr)].ttl = 7200

# sends the active peer list to the peer that requested it

def sendActivePeerList(peer):
    global peerList
    activePeers = ""
    cnt = 0
    for i in peerList.keys():
        if peerList[i].isActive == 1:
            cnt += 1
            activePeers += str(peerList[i].hostname) + ":" + str(peerList[i].port) + "\n"
    log(DBG, activePeers)

    if cnt == 0:
        peer.send(composeMsgResp("OK PQuery", ""))
    else:
        peer.send(composeMsgResp("OK PQuery", str(activePeers)))

def composeError(msgType):
    return "Error: " + msgType + " msg"


def alreadyRegistered(peerMsg):
    for i, j in peerList.iteritems():
        if j.hostname == peerMsg[1] and j.port == peerMsg[2]:
            return i
    return 0
# the class that handles each peer connection to the RS in a separate thread

class handleConnection(Thread):
    def __init__(self, peer, addr):
        Thread.__init__(self)
        self.peer = peer
        self.addr = addr
    def run(self):
        global peerList
        global cookieCount
        peerMessage = self.peer.recv(HDR_SIZE)
        #print "Message received:\n\"" + peerMessage
        (size, mType) = parseMsgHdr(peerMessage)
        #peerMessage = peerMessage.splitlines()
        #print peerList
        if size > 0:
            peerMessage = self.peer.recv(size)
            #print peerMessage +"\"\n"
            log(DBG,peerMessage+"\"\n")
            peerMessage = peerMessage.splitlines()
        if mType == "Register":
        #if peerMessage[0] == "Register":
            mutex.acquire()
            c = alreadyRegistered(peerMessage)
            #print "c is " + str(c)
            if peerMessage[0] == "-1" and c == 0:

                # register the peer with the RS
                registerPeer(self.addr, peerMessage)
                log(DBG, "Peer is registered..." + str(peerList[cookieCount].port))
                self.peer.send(composeMsgResp("OK Register", str(cookieCount)))
                cookieCount += 1
            elif (c != int(peerMessage[0]) and peerMessage[0] != "-1"):
                log(DBG, "Error... Invalid cookie..." + peerMessage[0])
                self.peer.send(composeMsgResp("Error Register", peerMessage[0]))
            else:
                log(DBG, "Peer is already registered...")
                peerList[c].isActive = 1
                peerList[c].ttl = 7200
                peerList[c].lastRegisteredTime = time.strftime('%H:%M:%S')
                peerList[c].hostname = peerMessage[1]
                peerList[c].port = peerMessage[2]
                peerList[c].activeCount += 1
                self.peer.send(composeMsgResp("OK Register", str(c)))
            # close the connection after it's registration is complete
            mutex.release()
            self.peer.close()
        
        if mType == "Leave":
            
            # check if the peer is in the peer list

            if peerMessage[0] != "-1":
                
                if peerList[int(peerMessage[0])].isActive == 1:
                    # delete that peer entry from the peer list
                    deactivatePeer(peerMessage[0])
                    #print "peer deactivated..."
                    self.peer.send(composeMsgResp("OK Leave", peerMessage[0]))
                
            # connection is closed after the peer is removed
            else:
                self.peer.send(composeMsgResp("Error", peerMessage[0]))
            self.peer.close()

        if mType == "PQuery":
            #print "Received PQuery..."
            # check if peer is in the peer list

            if peerMessage[0] != "-1" and peerList[int(peerMessage[0])].isActive == 1:
        
                # send active Peers list - to be implemented
                sendActivePeerList(self.peer)
            
            else:
                self.peer.send(composeMsgResp("Error", peerMessage[0]))
            self.peer.close()

        if mType == "KeepAlive":
            if peerMessage[0] != "-1":
                
                # reset the ttl for this peer to 7200
                updatePeerTTL(peerMessage[0])
                #print "Updated TTL..."
                self.peer.send(composeMsgResp("OK KeepAlive", peerMessage[0]))
            else:
                self.peer.send(composeMsgResp("Error", peerMessage[0]))
            # close the connection to the peer
            self.peer.close()
        #print peerList
def main():
    
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    log(DBG, "socket created successfully")

    s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    s.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,pack("ii",0,0))
    s.bind(('', PORT))
    log(DBG, str("socket bound to %s" %(PORT)))
    log(INFO, "Started Registration Server\n")
    log(INFO, "RS Hostname: "+ str(socket.gethostname())+" Port: "+ str(PORT)+"\n")
    s.listen(MAX_CLIENTS)

    #print "socket is listening for a peer request..."

    while True:
        # accepts a peer connection request
        peer, addr = s.accept()
        log(DBG, "connection request from " + str(addr[0]) +" ("+str(addr[1]) + ")")
        conn = handleConnection(peer, addr)
        conn.start()


if __name__ == "__main__":
    main()

