# Note : The analysis here is done only for the RANDOM.txt(Random method) logs file.... a similar analysis can be done using LL(Least Loaded) and RR(Round Robin methods too) 
time_1=[]
time_2=[]
f=open("RANDOM.txt","r")
line=f.readline()
while(line):
    line=f.readline()
    content=line.strip("\n").split(" ")
    if len(content)==3:
        time_1.append(float(content[-1]))
    if len(content)==2:
        time_2.append(float(content[-1]))
f.close()
mean_tasks=(sum(time_1)/len(time_1))
mean_jobs=(sum(time_2)/len(time_2))
time_1.sort()
time_2.sort()
mid_1=len(time_1)//2
mid_2=len(time_2)//2
median_tasks=(time_1[mid_1]+time_1[~mid_1])/2
median_jobs=(time_2[mid_2]+time_2[~mid_2])/2
print("The mean time of tasks is",mean_tasks)
print("The median time of tasks is",median_tasks)
print("The mean time of jobs is",mean_jobs)
print("The median time of jobs is",median_jobs)

a_1 = []
a_2 = []
a_3 = []
time = []
with open("RANDOM.txt") as f:
  content = f.readlines()
  content = [x.strip() for x in content]
  if len(content) >= 4:
    a_1.append(content[0])
    a_2.append(content[1])
    a_3.append(content[2])
    time.append(content[-1])

print(a_2)

a_1 = []
a_2 = []
a_3 = []
time = []
f = open("/content/RANDOM.txt","r")
line = f.readline()
while line:
    line = f.readline()
    print(line)
    content = line.strip("\n").split(" ")
    if len(content) >= 4:
      print(content)
      a_1.append(int(content[0]))
      a_2.append(int(content[1]))
      a_3.append(int(content[2]))
      time.append(float(content[-1]))
f.close()

time[0]

for i in range(len(time)):
  time[i] = time[i]*0.000000001
print(time[0:20])

len(time)

import matplotlib.pyplot as plt
plt.step(time,a_1,"r")
plt.step(time,a_2,"b")
plt.step(time,a_3,"g")
plt.plot()


        
