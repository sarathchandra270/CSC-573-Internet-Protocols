
import socket
import sys

next_seq_no=0
HEADER_LEN = 8
ACK_ID = 0b1010101010101010


s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
address=("192.168.0.23", 5010)
s.bind(address)


r_port=int(sys.argv[1])
w_file=str(sys.argv[2])
loss=float(sys.argv[3])

#function to calculate carry-around
def carry_around_add(a, b):
    c = a + b
    return (c & 0xffff) + (c >> 16)

#function to compute checksum
def checksum(msg):
    # Force data into 16 bit chunks for checksum
    if (len(msg) % 2) != 0:
        msg += "0"

    s = 0
    for i in range(0, len(msg), 2):
        w = ord(msg[i]) + (ord(msg[i+1]) << 8)
        s = carry_around_add(s, w)
    return ~s & 0xffff

#function to parse the packet
def parse_pkt(file_bytes):
    pkt_unpacket = unpack('iHH' + str(len(file_bytes)-HEADER_LEN) + 's', file_bytes) + (False,)
    return pkt_unpacket

#function to send ack
def send_ack(seq_num, host):
    raw_ack = pack('iHH', seq_num, 0, ACK_ID)
    s.sendto(raw_ack, (host,r_port))


while True:
    file_bytes, addr = s.recvfrom(1024)
    print "receiving data"

    #parsing the received packet
    prase_file_bytes = parse_pkt(file_bytes)

    #compute checksum
    value=check_sum(prase_file_bytes.data)

    #check received checksum and sent  checksum are same. if same check, then check seq no
    if(value==prase_file_bytes.chk_sum):


        #check if in-sequence 
        if(next_seq_no==prase_file_bytes.seq_num):

            #if in-sequence send ack & write data to file
            send_ack(prase_file_bytes.seq_num, addr[0])
            next_seq_no +=1

            new_file=open(w_file,'a+')
            new_file.write(prase_file_bytes.data)
            new_file.close()

        #else send ack of last packet
        else:
            send_ack(next_seq_no, addr[0])

    #wrong checksum, then do nothing
    else:
        continue
    



