# Databricks notebook source
# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/3869542895595336/2709359236233034/latest.html

x=10

a=0

if(a>x):
  print("Hello")
elif(a==x):
  pass
  #print("value found")
else:
  print("Nothing Found")

# COMMAND ----------

30 is (20+10)


# COMMAND ----------

300 is (200+100)

# COMMAND ----------

# () -> [] -> bitwise -> relational -> logical operator

a = 0.1 + 0.2

print(a)
type(a)

# COMMAND ----------

my_list = ["Java",["1.7","1.8","1.9"],"Python",["3.7","3.8"],"Android Studio",["2.7","3.0","3.1"]]

len(my_list)

# COMMAND ----------

data = [10, 20, 30]

data.append(40)

print(data)

data.append( [50,60] )

print("append data:",data)

data.extend([100,110])

print("extend data:", data)

data.insert(1,10000)

print("insert data:",data)

# COMMAND ----------

#id(data[0]) #11167936

print("x memory", id(x))

# COMMAND ----------

aDict = { "name":"deepak", "age":21, 142:"roomNo"}

#print(aDict["name"])

print(aDict)

#insert new element
aDict["city"] = "manali"

aDict["name"] = "mokshda"

print(aDict)

# COMMAND ----------

for i in aDict:
  print(aDict[i])

# COMMAND ----------

for k,v in aDict.items():
  print(k," : ",v)

# COMMAND ----------

a={1,2,3,1}

print(a, type(a))

b = { 1,2,3,"mokshda",(5,6,7) }

print(b)

# COMMAND ----------

#create empty set

c = set()

print(type(c))

# COMMAND ----------

b.discard(3)

print(b)

# COMMAND ----------

a={1,2,3,4,5}
b={3,4}

#a.isdisjoint(b)

b.issubset(a)

# COMMAND ----------

def secondFxn(name):
  print("Name memory is:", id(name))
  print("My name that's passed is: ", name)
  
data = "Deepak"
print("data memory:", id(data))

secondFxn(data)  

# COMMAND ----------

def swapping(a,b):
  a,b = b,a
  
x=10
y=20

print("Function before calling x :", x, "y :", y)

swapping(x,y)
print("Function before calling x :", x, "y :", y)

# COMMAND ----------

def variedFxn(year=2020, *name):
  
  for i in name:
    print("Names are:", i,"and the year is:",year)
    
variedFxn( "Deepak", "Mokshda", "Kamal", "Anshul" ) 

# COMMAND ----------

def allName(**kwargs):
  print(type(kwargs))
  print(kwargs)
  for i,j in kwargs.items():
    print(i," : ",j)
  
  #print("First name: ",fname,\
  #     "\nLast name: ",lname,\
  #"\nFull Name(first+last): ",fullName)
  
allName(fname="Deepak",lname="Saini",fullName="Deepak Saini", age=21)  

# COMMAND ----------

   #LAMBDA FUNCTION
  
x = lambda a,b : a*b
  
x(10,20)  

# COMMAND ----------

list1 =(1,2,3)

out = list(map(lambda x : x*2 , list1))

print(out)

# COMMAND ----------

        #FILE HANDLING
  
fileobj = open( "filename", "accessMode" )   #basic syntaxfor opening a file  

# COMMAND ----------


