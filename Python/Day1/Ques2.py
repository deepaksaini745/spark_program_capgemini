# Databricks notebook source
#https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/2192379573512764/2709359236233034/latest.html


#w1h = float(input("Enter the height of wall 1 in feet and inches:"))
w1h=10.9
w1hm = ( int(w1h)*12 + (w1h - int(w1h)) )*0.0254 

#w1w = float(input("Enter the width of wall 1 in feet and inches:"))
w1w=12.5
w1wm = ( int(w1w)*12 + (w1w - int(w1w)) )*0.0254 

#w2h = float(input("Enter the height of wall 2 in feet and inches:"))
w2h=11.6
w2hm = ( int(w2h)*12 + (w2h - int(w2h)) )*0.0254 

#w2w = float(input("Enter the width of wall 2 in feet and inches:"))
w2w=15.8
w2wm = ( int(w2w)*12 + (w2w - int(w2w)) )*0.0254 

sqmt1 = w1hm * w1wm

sqmt2 = w2hm * w2wm

rate = 120.0

rate1 = round(sqmt1 * rate, 2)
rate2 = round(sqmt2 * rate, 2)
total = round(rate1 + rate2, 2)

print("Total cost of painting both walls is Rs. {0}".format(total))
print("Cost of painting wall 1 is Rs. {0} and wall 2 is Rs. {1}".format(rate1, rate2))
