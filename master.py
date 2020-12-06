import threading
import json
import socket
import time
import sys
import random
from math import inf
#varables initialization
JOB_PORT=5000
HEADER=100000
SERVER='localhost'
JOB_ADDR=(SERVER,JOB_PORT)
FORMAT="utf-8"
JOB_DICT={}
WORKERS_MATRIX=[]
JOB_MATRIX={}
#WORKER_PORTS_BASE=[0]
TRASH_DICT={"TEMP_COUNT":0, "WORKER_FULL":0,"TOT_SLOTS":0,"NO_WORKERS":0}
lock=threading.Lock()
QUEUE=[]
WORKER_INDEX={}
#confing file , no_workers,slots initialization
def init_work_matrix(config_info,lock):
    iterable=config_info["workers"]
    lock.acquire()
    TRASH_DICT["NO_WORKERS"]=len(iterable)
    for i in range(TRASH_DICT["NO_WORKERS"]):
            QUEUE.append(i)
            TRASH_DICT["TOT_SLOTS"]+=config_info["workers"][i]["slots"]
            WORKER_INDEX[config_info["workers"][i]["worker_id"]]=i
    lock.release()
    for i in iterable:
        no_slots=int(i["slots"])
        lock.acquire()
        WORKERS_MATRIX.append([0 for j in range(no_slots)])
        lock.release()


config_file=open("config.json",)
config_info=json.load(config_file)
#print("config info ",config_info,"\n\n\n")
init_work_matrix(config_info,lock)
#print("initial TRASH_DICT",TRASH_DICT,"\n\n\n")
#print("initial WORKERS_MATRIX",WORKERS_MATRIX,"\n\n\n")

#JOB_DICT , JOB_MATRIX initialization
def getting_job_request(lock):
    while True:
        sock,addr=master_job_recive_sock.accept()
        msg=sock.recv(HEADER).decode(FORMAT)
        if msg:
            msg=json.loads(msg)
            lock.acquire()
            JOB_DICT[msg["job_id"]]=msg
            JOB_MATRIX[msg["job_id"]]=[len(JOB_DICT[msg["job_id"]]["map_tasks"]),0,len(JOB_DICT[msg["job_id"]]["reduce_tasks"]),0,0,0]
            #print("jobbb",len(JOB_DICT[msg["job_id"]]["map_tasks"]),len(JOB_DICT[msg["job_id"]]["reduce_tasks"]))
            lock.release()


master_job_recive_sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
master_job_recive_sock.bind(JOB_ADDR)
master_job_recive_sock.listen()
job_request_thread=threading.Thread(target=getting_job_request,args=(lock,))
job_request_thread.start()


def task_updation(lock):
    count=1
    while True:
        WORKER_MAIN,WOR_ADDR=SENDING_SOCKET.accept()
        DATA=WORKER_MAIN.recv(2048)
        if DATA:
            DATA=json.loads(DATA.decode())
            print("DATA RECIEVED",DATA,"COUNT",count)
            count+=1
            lock.acquire()
            #print(WORKER_INDEX[DATA["worker"]],"WORKER MATRIX",WORKERS_MATRIX)
            #print(WORKERS_MATRIX[WORKER_INDEX[DATA["worker"]]])
            #print(DATA["slot"])
            WORKERS_MATRIX[WORKER_INDEX[DATA["worker"]]][DATA["slot"]]=0
            if int(DATA["map_red"])==1:
                JOB_MATRIX[DATA["job"]][1]+=1
            else:
                JOB_MATRIX[DATA["job"]][3]+=1
            if  JOB_MATRIX[DATA["job"]][2]==JOB_MATRIX[DATA["job"]][3]:
                del JOB_MATRIX[DATA["job"]]
            TRASH_DICT["TEMP_COUNT"]-=1
            TRASH_DICT["WORKER_FULL"]=0
            
            lock.release()

#random allocation
def random_allocation(send):
    while True:
        flag=0
        i=random.randrange(0,TRASH_DICT["NO_WORKERS"])
        for j in range(len(WORKERS_MATRIX[i])):
            if WORKERS_MATRIX[i][j]==0:
                #print(send,"sending to",i,j)
                CURRENT_WORKER=config_info["workers"][i]["port"]
                CURRENT_WORKER_ADDR=(SERVER,CURRENT_WORKER)
                WORKER_SOCKET=socket.socket(socket.AF_INET ,socket.SOCK_STREAM)
                WORKER_SOCKET.connect(CURRENT_WORKER_ADDR)
                message=json.dumps({"slot":j,"time":send["time"],"job":send["job_id"],"map_red":send["map_red"],"task":send["task_id"]})
                WORKER_SOCKET.send(message.encode())
                WORKER_SOCKET.close()
                #print("message sent\n\n\n")
                WORKERS_MATRIX[i][j]=1
                TRASH_DICT["TEMP_COUNT"]+=1
                #print("tot slots ",TRASH_DICT["TOT_SLOTS"],"temp count",TRASH_DICT["TEMP_COUNT"])
                if TRASH_DICT["TEMP_COUNT"]==TRASH_DICT["TOT_SLOTS"]:
                    TRASH_DICT["WORKER_FULL"]=1
                if int(send["map_red"])==1:
                    JOB_MATRIX[send["job_id"]][4]+=1
                else:
                    JOB_MATRIX[send["job_id"]][5]+=1
                '''else:
                    for l in range(100):
                        print("ERROR")'''
                '''if flag==0 and JOB_MATRIX[send["job_id"]][4]==JOB_MATRIX[send["job_id"]][0]:
                    print("\n\n\n\n\n\n\n\nJOB ID BECAME ZERO YAYYYYY\n\n\n\n\n\n\n\n")
                    print(JOB_MATRIX[send["job_id"]],TRASH_DICT,"\n\n\n\nPRINTING")
                    flag=1'''
                return
def round_robin(send):
    while True:
        i=QUEUE[0]
        for j in range(0,len(WORKERS_MATRIX[i])):
            if WORKERS_MATRIX[i][j]==0:
                #print(send,"sending to",i,j)
                CURRENT_WORKER=config_info["workers"][i]["port"]
                CURRENT_WORKER_ADDR=(SERVER,CURRENT_WORKER)
                WORKER_SOCKET=socket.socket(socket.AF_INET ,socket.SOCK_STREAM)
                WORKER_SOCKET.connect(CURRENT_WORKER_ADDR)
                message=json.dumps({"slot":j,"time":send["time"],"job":send["job_id"],"map_red":send["map_red"],"task":send["task_id"]})
                WORKER_SOCKET.send(message.encode())
                WORKER_SOCKET.close()
                #print("message sent\n\n\n")
                WORKERS_MATRIX[i][j]=1
                TRASH_DICT["TEMP_COUNT"]+=1
                #print("tot slots ",TRASH_DICT["TOT_SLOTS"],"temp count",TRASH_DICT["TEMP_COUNT"])
                if TRASH_DICT["TEMP_COUNT"]==TRASH_DICT["TOT_SLOTS"]:
                    TRASH_DICT["WORKER_FULL"]=1
                if int(send["map_red"])==1:
                    JOB_MATRIX[send["job_id"]][4]+=1
                else:
                    JOB_MATRIX[send["job_id"]][5]+=1
                return
        QUEUE.remove(i)
        QUEUE.append(i)
def least_used(send):
    while True:
        min_sum=inf
        i=0
        for l in range(TRASH_DICT["NO_WORKERS"]):
            if 0 in WORKERS_MATRIX[l]:
                if sum(WORKERS_MATRIX[l])<min_sum:
                    i=l
                    min_sum=sum(WORKERS_MATRIX[l])
        for j in range(0,len(WORKERS_MATRIX[i])):
            if WORKERS_MATRIX[i][j]==0:
                #print(send,"sending to",i,j)
                CURRENT_WORKER=config_info["workers"][i]["port"]
                #print(CURRENT_WORKER,"CURRENT WORKER")
                CURRENT_WORKER_ADDR=(SERVER,CURRENT_WORKER)
                WORKER_SOCKET=socket.socket(socket.AF_INET ,socket.SOCK_STREAM)
                WORKER_SOCKET.connect(CURRENT_WORKER_ADDR)
                message=json.dumps({"slot":j,"time":send["time"],"job":send["job_id"],"map_red":send["map_red"],"task":send["task_id"]})
                WORKER_SOCKET.send(message.encode())
                WORKER_SOCKET.close()
                #print("message sent\n\n\n")
                WORKERS_MATRIX[i][j]=1
                TRASH_DICT["TEMP_COUNT"]+=1
                #print("tot slots ",TRASH_DICT["TOT_SLOTS"],"temp count",TRASH_DICT["TEMP_COUNT"])
                if TRASH_DICT["TEMP_COUNT"]==TRASH_DICT["TOT_SLOTS"]:
                    TRASH_DICT["WORKER_FULL"]=1
                if int(send["map_red"])==1:
                    JOB_MATRIX[send["job_id"]][4]+=1
                else:
                    JOB_MATRIX[send["job_id"]][5]+=1
                return
        
#mapper and reducer message sending
def task_allocation_maper(lock):
        while TRASH_DICT["WORKER_FULL"]:
           err=1
        lock.acquire()

        while True:
            i="EMPTY"
            temp=0
            if JOB_MATRIX:
               for job in JOB_MATRIX:
                   if  JOB_MATRIX[job][0]>JOB_MATRIX[job][4]:
                        temp=1
                        i=job
                        break 
            if  temp==0:
               i="ALL MAPS DONE"
            if i!="EMPTY" and i!="ALL MAPS DONE":
                send={"job_id":i,"map_red":1,"task_id":JOB_MATRIX[i][4],"time":int(JOB_DICT[i]["map_tasks"][JOB_MATRIX[i][4]]["duration"])}
                #print(JOB_MATRIX,"JOB_MATRIX CAME FOR MAPPER",send,"sending dict")
                if sys.argv[1]=="RANDOM":
                    random_allocation(send)
                elif sys.argv[1]=="RR":
                    round_robin(send)
                elif sys.argv[1]=="LL":
                    least_used(send)
                else:
                    print("\n\n\n\n\n\n\n\nWRONG INPUT \n\n\n\n\n\n\n")
            lock.release()
            while TRASH_DICT["WORKER_FULL"]:
                   err=1
            lock.acquire()
                
                
def task_allocation_reducer(lock):
    while TRASH_DICT["WORKER_FULL"]:
        err=1
    lock.acquire()
    while True:
        i="EMPTY"
        temp=0
        if JOB_MATRIX:
            for job in JOB_MATRIX:
                if JOB_MATRIX[job][0]==JOB_MATRIX[job][1] and JOB_MATRIX[job][2]>JOB_MATRIX[job][5]:
                    temp=1
                    i=job
                    break
            if temp==0:
                i="ALL REDUCERS DONE"
            if i!="EMPTY" and i!="ALL REDUCERS DONE":
                send={"job_id":i,"map_red":2,"task_id":JOB_MATRIX[i][5],"time":int(JOB_DICT[i]["reduce_tasks"][JOB_MATRIX[i][5]]["duration"])}
                if sys.argv[1]=="RANDOM":
                    random_allocation(send)
                elif sys.argv[1]=="RR":
                    round_robin(send)
                elif sys.argv[1]=="LL":
                    least_used(send)
                else:
                    print("\n\n\n\n\n\n\n\nWRONG INPUT \n\n\n\n\n\n\n")
        lock.release()
        while TRASH_DICT["WORKER_FULL"]:
                err=1
        lock.acquire()

SENDING_SOCKET=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
SENDING_SOCKET.bind(('localhost',5001))
SENDING_SOCKET.listen()
task_allocation_thread_maper=threading.Thread(target=task_allocation_maper,args=(lock,))
task_allocation_thread_maper.start()
task_allocation_thread_reducer=threading.Thread(target=task_allocation_reducer,args=(lock,))
task_allocation_thread_reducer.start()
task_updation_thread=threading.Thread(target=task_updation,args=(lock,))
task_updation_thread.start()
