import socket
import sys

HEADER_LEN = 8
DATA_ID = 0b0101010101010101



#creating UDP socket
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
address=("192.168.0.23", 5010)
s.bind(address)

#assigning names to data entered though Command Line
s_rcv1=str(sys.argv[1])
s_rcv2=str(sys.argv[2])
s_port=int(sys.argv[3])
f_name=str(sys.argv[4]
MSS=int(sys.argv[5])


#reading data from file
f_object=open("f_name","r")
f_data=f_object.read()
f_object.close()

#sending data in file
rdt_send(f_data)



def build_pkts(f_data):
    seq_no=0
    pkts=[]
    sent=0
    to_sent=min(MSS-header_len, len(f_data)-sent)
    while to_sent>0:
        pkts.append(pkt(seq_num=seq_no, chk_sum=checksum(f_data[sent:sent+to_sent]), pkt_type=pkt_ID, data=f_data[sent:sent+to_sent]))
        sent = sent + to_sent
        to_sent = min(MSS-header_len, len(f_data)-sent)
        seq_no = seq_no+1

    return pkts

def rdt_send(f_data):
    #make packet list
    pkts=build_pkts(f_data);
	
	
				













if __name__=="__main__":
    main(sys.argv[1:])
