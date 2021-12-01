import sqlite3
import time

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

time_b = time.time()
con = sqlite3.connect("tmp2.db") #打开数据库
con.row_factory = dict_factory
c = con.cursor() #创建游标对象

print("t1:{}".format(time.time()-time_b))
time_b = time.time()

c.execute("select name from sqlite_master ")
# """select name from sqlite_master where type='table' order by name"""
output = c.fetchall()
print(output)

print("t2:{}".format(time.time()-time_b))
time_b = time.time()

# [{'name': 'ECDict'}, {'name': 'sqlite_sequence'}, {'name': 'COMPANY'}, {'name': 'sqlite_autoindex_COMPANY_1'}]
# [{'name': 'ECDict'}, {'name': 'sqlite_sequence'}, {'name': 'ID'}, {'name': 'Word_INDEX'}]
c.execute("select * from usermessage")
# c.execute("select * from ECDict limit 2029,1")
output = c.fetchall()
# for i in range(10):
#     output = c.fetchone()
print(output)
print("t3:{}".format(time.time()-time_b))