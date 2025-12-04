import re
import csv
import pandas as pd
import numpy as np

#Remove extra lines from the script.
file=open(r'test_script.txt','r+')
first=file.readlines()
file1=open(r'first_draft.txt','w')
for line in first:
    if line.strip():
        file1.write(line)
file1.close()
file.close()

#Remove leading space from the script.
file=open(r'first_draft.txt','r+')
second=file.readlines()
file1=open(r'second_draft.txt','w')
for line in second:
    if line.lstrip(" "):
        file1.write(line.lstrip()) 
file1.close()
file.close()

#Remove comments from the script
file=open(r'second_draft.txt','r+')
third=file.readlines()
file1=open(r'third_draft.txt','w')
for line in third:
    if line[0][0]!='#':
        file1.write(line)
file1.close()
file.close()

#Removing content before def function and writing actual code.
function_flag=0
file=open(r'third_draft.txt','r+')
fourth=file.readlines()
file1=open(r'fourth_draft.txt','w')
for line in fourth:
    if line.split(' ')[0]=='def':
        function_flag=1
    elif function_flag==1 and line.split(' ')[0]!='def':
        file1.write(line)
file1.close()
file.close()


#Create a dictionary with dataframe name as key and query as values.
file1=open(r'fourth_draft.txt','r+')
text=""
content=file1.readlines()
for line in content:
    text+=line
#Regex to split df name and query from script.
matches = re.findall(r'(\w+)\s*=\s*(spark\.sql\(\s*f?("""(?:.|\n)*?""")\s*(?:\.format\([^\)]*\))?\s*\)|\w+(?:\.(?:unionAll|union_by_name)\(\s*\w+\s*\))+|\w+(?:\.(?:unionAll|union_by_name)\(\s*\w+(?:\.(?:unionAll|union_by_name)\(\s*\w+\s*\))*\s*\)))',text)
#print(matches)
data={}
for df_name, sql_query,value in matches:
    data[df_name]=sql_query

print(data)

for i in data.keys():
    if i=='r_geo_funcl_hiery_temp_df':
        print("Inside if")
        print(data[i])
source={}
for i in data.keys():
  #To get Name of source table and Schema
  matches = re.findall(r'(?:from|join)\s+(?:([{a-zA-Z_][a-zA-Z0-9_]*}?)\.)?([a-zA-Z_][a-zA-Z0-9_]*)', data[i], re.IGNORECASE)
  columns_block = re.search(r'(?i)select\s+([\s\S]*?)\s+from\b', data[i])

  source[i]={}
  source[i]['Name']=[]
  source[i]['Schema']=[]
  source[i]['Columns']=[]
  for j in matches:
    dp=list(j)
    if dp[0]=='':
      dp[0]='df'
      source[i]['Schema'].append(dp[0])
      source[i]['Name'].append(dp[1])
    else:
      source[i]['Schema'].append(dp[0])
      source[i]['Name'].append(dp[1])
    if columns_block:
      columns_only = columns_block.group(1).strip()
      source[i]['Columns']=columns_only.split(',')
#print(len(list(source.keys())))
excel={}
for i in source.keys():
   excel[i]=source[i]['Name']
# join=re.finditer(r'(?i)(inner|left(?: outer)?|right(?: outer)?|full(?: outer)?|cross)?\s*join\s+([^\s]+)', data[i])
# for match in join:
#   print(match.group(1))
# for i in excel.keys():
#    print(f"{i} : {excel[i]}")