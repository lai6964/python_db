# coding: utf-8
import pandas as pd
import os
import sqlite3


class Table_OP:
    """创建表格，插入数据"""
    def __init__(self, database_path):
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, *args):
        """创建表
        :param table_name:表名
        :param args: 表元素定义
        """
        table_seg1 = ""
        for i in args:
            table_seg1 += i
            if i != args[len(args) - 1]:
                table_seg1 += ","
            # print(i)
        create_tab_sql = "CREATE TABLE IF NOT EXISTS %s (%s)" % (table_name, table_seg1)
        # 这里的问题是：使用不定长参数写入字段，python传入的是一个元组，建表语句的字符串会有引号；问题就在于使用什么方法将args元组内的引号去除
        # 使其成为MySQL可执行的语句。(解决思路：序列拼接（相加）)
        try:
            print("Done:",create_tab_sql)
            self.cursor.execute(create_tab_sql)
        except:
            print("创建表出错，请检查！")

    def close_link(self):
        '''关闭连接'''
        self.cursor.close()
        self.conn.close()

    def insert_table_row(self, table_name, dict1):
        """插入数据的方法
        :param table_name:表名
        :param dict1: 插入的字典
        """
        keys = ','.join(dict1.keys())#表中元素名
        values = ', '.join(['?'] * len(dict1.keys()))#表中元素
        insert_data_sql = 'INSERT INTO {table_name}({keys}) values ({value})'.format(table_name=table_name, keys=keys, value=values)
        try:
            self.cursor.execute(insert_data_sql, tuple(dict1.values()))
            self.conn.commit()
            print("Done:",'INSERT INTO {table_name}({keys}) values ({value})'.format(table_name=table_name, keys=keys, value=tuple(dict1.values())))
        except:
            print('fail!')
            self.conn.rollback()
        # print(insert_data_sql)

    def insert_tablemany_row(self, table_name, list_rows):
        '''将多条数据插入'''
        # insert_many_sql = "INSERT INTO %s values (%r,%r,%r,%r)"%(table_name, list_rows)
        # print(insert_many_sql)
        try:
            self.cursor.executemany("INSERT INTO {} values (?,?,?,?)".format(table_name), list_rows)
            self.conn.commit()
        except:
            print("fail!")
            self.conn.rollback()

    def drop_table(self, table_name):
        '''删除表的方法'''
        drop_table_sql = "DROP TABLE IF EXISTS %s" % table_name
        # backup_table_sql = "MYSQLDUMP -u root -p {databse} {table_name} > {tabel_name_bck} ".format(
        #     databse='usermessage', table_name=table_name, tabel_name_bck=table_name + 'bck.sql')
        self.cursor.execute("select name from sqlite_master")
        tables = self.cursor.fetchall()  # 列出所有表名
        print(tables)
        try:
            for i in tables:
                if i[0] == table_name:#删除指定表名
                    # self.cursor.execute(backup_table_sql)  #备份表会出错，问题暂时未知
                    self.cursor.execute(drop_table_sql)
                    print("Done:",drop_table_sql)
                else:
                    pass#print("此前未新建过相同表")
        except:
            print("删除表失败！")

    def insert_table_col(self, table_name, new_col_name):
        insert_table_sql = "alter table {} add column {}".format(table_name, new_col_name)
        try:
            self.cursor.execute(insert_table_sql)
            print("Done:", insert_table_sql)
        except:
            print("fail!")
            self.conn.rollback()

    def update_table_element(self, table_name, element, condition):
        key = tuple(element.keys())[0]
        value = tuple(element.values())[0]
        key2 = tuple(condition.keys())[0]
        value2 = tuple(condition.values())[0]
        update_table_sql = "update {} set {}={} where {}={}".format(table_name, key, value, key2, value2)
        try:
            self.cursor.execute(update_table_sql)
            self.conn.commit()
            print("Done:",update_table_sql)
        except:
            print("fail!")
            self.conn.rollback()

    def drop_table_row(self, table_name, element):
        drop_table_sql = "delete from {} where {}={}".format(table_name, tuple(element.keys())[0], tuple(element.values())[0])
        try:
            self.cursor.execute(drop_table_sql)
            self.conn.commit()
            print("Done:",drop_table_sql)
        except:
            print("fail!")
            self.conn.rollback()

if __name__ == '__main__':
    database_path = 'poetry.db'
    if os.path.exists(database_path):
        os.remove(database_path)



    # 获取xlsx文件行号，所有列名称
    df = pd.read_excel('poetry.xlsx')  # 读取xlsx中第一个sheet
    # print("输出行号列表{}".format(df.index.values))  # 获取xlsx文件的所有行号
    print("输出列标题{}".format(df.columns.values))  # 所有列名称

    # 读取xlsx数据转换为字典
    test_data = []
    for i in df.index.values:  # 获取行号的索引，并对其进行遍历：
        # 根据i来获取每一行指定的数据 并利用to_dict转成字典
        row_data = df.iloc[i, :].to_dict()
        # print(row_data)
        test_data.append(row_data)


    table = Table_OP(database_path)
    table_name = 'poetry'
    table_seg = ("WorkId int primary key",
                 "Title text",
                 "Author text",
                 "Dynasty text",
                 "Foreword text",
                 "Content text",
                 "Kind text",
                 "Introduce text",
                 "Annotation text",
                 "Translation text",
                 "Appreciation text",
                 "Layout text",
                 "Wiki text",
                 "FileKey text",
                 "Types text",
                 "Tags text",
                 "flag text",
                 "grad text",
                 "book text",
                 "music text")

    table.create_table(table_name, *table_seg)  # 不定长参数：定义组成元组，调用解元组
    import json
    for dict1 in test_data:
        dict1.update({'WorkId':int(dict1['WorkId'])}) #这个不知为何是int64格式的，导致写入后读取格式不对，强制转换
        table.insert_table_row(table_name, dict1)

    # 关闭数据库连接
    table.close_link()