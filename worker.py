import threading
import json
import socket
import time
import sys
import random
import queue
SENDING_ADDR=('localhost',5001)

def wait_time_delay(MESSAGE, work_id):
    SENDING_SOCKET=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    SENDING_SOCKET.connect(('localhost',5001))
    time.sleep(int(MESSAGE["time"]))
    MESSAGE["worker"]=work_id
    SENDING_SOCKET.send(json.dumps(MESSAGE).encode())
    #print("sent ",MESSAGE)

def worker_function(port,work_id):
    #print("worker initialized",port,work_id)
    CUR_WORK_SOCKET=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    CUR_WORK_SOCKET.bind(('localhost',port))
    CUR_WORK_SOCKET.listen()
    while True:
        MASTER_SOCKET,MASTER_ADDR=CUR_WORK_SOCKET.accept()
        print("master connection accepted",port,MASTER_ADDR)
        MESSAGE=json.loads(MASTER_SOCKET.recv(2048).decode("utf-8"))
        TEMP_THREAD=threading.Thread(target=wait_time_delay,args=(MESSAGE,work_id))
        TEMP_THREAD.start()


worker_thread=threading.Thread(target=worker_function,args=(int(sys.argv[1]),sys.argv[2]))
worker_thread.start()
                                                                            
