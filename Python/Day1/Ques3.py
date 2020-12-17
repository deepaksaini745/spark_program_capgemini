# Databricks notebook source
# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/2192379573512766/2709359236233034/latest.html


#time = int(input("Enter the time in seconds: "))
time = 3599

h = int(time/3600)
m = int( (time - (h*3600))/60 )
s = (time - ((h*3600)+(m*60)))

if(h<10 or m<10 or s<10):
  if(h<10):
    req = ("0{0}:{1}:{2}".format(h,m,s))
    if(m<10):
      req = ("0{0}:0{1}:{2}".format(h,m,s))
      if(s<10):
        req = ("0{0}:0{1}:0{2}".format(h,m,s))
      else:
        pass
    else:
      pass
  else:
    req = ("{0}:{1}:{2}".format(h,m,s))
    if(m<10):
      req = ("{0}:0{1}:{2}".format(h,m,s))
      if(s<10):
        req = ("{0}:0{1}:0{2}".format(h,m,s))
      else:
        pass
    elif(s<10):
      req = ("{0}:{1}:0{2}".format(h,m,s))
    else:  
      pass         
else:
  req = ("{0}:{1}:{2}".format(h,m,s))  
  
print(req)  
