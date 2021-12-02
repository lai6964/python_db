import sqlite3
import time


import random
class MenMessage:
    '''
    利用randrange生成姓名长度；利用sample生成姓名样本
    '''
    def __init__(self, seg):
        self.seg = seg
    def name(self):
        # 生成姓名长度
        name_leng = random.randrange(4, 7)
        name = random.sample(self.seg, name_leng)
        return name
    def sex(self):
        # 性别：1：男；2：女
        sex = random.randrange(1, 3)
        if sex == 1:
            return '男'
        else:
            return '女'
    def age(self):
        age = random.randrange(18, 51)
        return age


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
    import os
    database_path = 'tmp2.db'
    if os.path.exists(database_path):
        os.remove(database_path)
    start = time.perf_counter()
    list_rows = []
    DATA = range(1, 11)
    table = Table_OP(database_path)

    # 新建表示例
    table_name = 'usermessage'  # 表名称
    table_seg = ("id int primary key", "name varchar(12) NOT NULL", "Email varchar(20)", "age int", "sex char(4)")
    table.create_table(table_name, *table_seg)  # 不定长参数：定义组成元组，调用解元组
    table.create_table("table_name", *table_seg)  # 不定长参数：定义组成元组，调用解元组

    # 对应表加行示例
    for i in DATA:
        men = MenMessage(
            seg=['W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X',
                 'C', 'V',
                 'B', 'N', 'M', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h', 'j',
                 'k', 'l',
                 'z', 'c', 'v', 'n', 'm', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0']
        )
        # 将列表强制转换成字符串
        name = ''.join(men.name())
        sex = men.sex()
        age = men.age()
        id = i
        char1 = '{"id": id, "name": name, "email": name+"@163.com" , "age": age, "sex": sex}'
        list_row = ((name, name + "@163.com", age, sex),)
        # print(type(list_row))
        list_rows += list_row
        # print(list_rows)
        # print(type(char1))
        dict1 = eval(char1)
        keys = ','.join(dict1.keys())
        values = ', '.join(['?'] * len(dict1.keys()))
        # print(keys)
        # print(values)
        table.insert_table_row(table_name, dict1)
    # table.insert_tablemany_row(table_name,list_rows)
    print("写入{DATA}条数据，耗时{time}秒".format(DATA=len(DATA), time=time.perf_counter() - start))


    # 对应表加列示例
    new_col_name = "score"
    table.insert_table_col(table_name,new_col_name)

    # 对应表更新元素
    dict1 = {"score":18}
    condition = {"id":9}
    table.update_table_element(table_name, dict1, condition)

    # 对应表删除行示例
    condition = {"id":10}
    table.drop_table_row(table_name, condition)

    # # 对应表删除示例
    # table.drop_table(table_name)


    # 关闭数据库连接
    table.close_link()