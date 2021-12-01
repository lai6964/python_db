import sqlite3
import time

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

if __name__ == '__main__':
    con = sqlite3.connect("tmp2.db")  # 打开数据库
    con.row_factory = dict_factory
    c = con.cursor()  # 创建游标对象

    # 查看表名
    c.execute("select name from sqlite_master ")
    output = c.fetchall()
    print(output)#[{'name': 'usermessage'}, {'name': 'usermessage2'}]

    # 查看表元素
    c.execute("select * from usermessage")
    output = c.fetchone()
    keys = tuple(output.keys())
    for k in keys:
        print(k)
    keys = ','.join(keys)
    print(keys)