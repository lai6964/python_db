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
    """
    创建表格，插入数据
    """
    def __init__(self, database_path):
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, *args):
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


    def insert_table(self, table_name, keys, values, dict1):  # https://www.cnblogs.com/xiao-xue-di/p/11570451.html
        '''插入数据的方法'''
        insert_data_sql = 'INSERT INTO {table_name}({keys}) values ({value})'.format(table_name=table_name, keys=keys, value=values)
        try:
            self.cursor.execute(insert_data_sql, tuple(dict1.values()))
            # print("插入数据成功！")
            self.conn.commit()
        except:
            print('fail!')
            self.conn.rollback()
        # print(insert_data_sql)

    def insert_tablemany(self, table_name, list_rows):
        '''将多条数据插入'''
        # insert_many_sql = "INSERT INTO %s values (%r,%r,%r,%r)"%(table_name, list_rows)
        # print(insert_many_sql)
        try:

            self.cursor.executemany("INSERT INTO usermessage values (%s,%s,%s,%s)", list_rows)
            self.conn.commit()
        except:
            print("fail!")
            self.conn.rollback()

    def close_link(self):
        '''关闭连接'''
        self.cursor.close()
        self.conn.close()

    def drop_table(self, table_name):
        '''删除表的方法'''
        drop_table_sql = "DROP TABLE IF EXISTS %s" % table_name
        # backup_table_sql = "MYSQLDUMP -u root -p {databse} {table_name} > {tabel_name_bck} ".format(
        #     databse='usermessage', table_name=table_name, tabel_name_bck=table_name + 'bck.sql')
        self.cursor.execute("select name from sqlite_master")
        tables = self.cursor.fetchone()  # 以序列的序列方式返回余下所有行
        # print(tables)
        try:
            for i in tables:
                if i == table_name:
                    # self.cursor.execute(backup_table_sql)  #备份表会出错，问题暂时未知
                    self.cursor.execute(drop_table_sql)
                # 表进行备份
                # 执行删除命令
                else:
                    print("此前未新建过相同表")
        except:
            print("删除表失败！")

if __name__ == '__main__':

    start = time.perf_counter()
    list_rows = []
    DATA = range(1, 101)
    database_path = 'tmp2.db'
    table_name = 'usermessage'  # 表名称
    table_seg = ('name varchar(12) NOT NULL', "Email varchar(20)", "age int", "sex char(4)")
    table = Table_OP(database_path)
    table.create_table(table_name, *table_seg)  # 不定长参数：定义组成元组，调用解元组
    table.create_table("usermessage2", *table_seg)  # 不定长参数：定义组成元组，调用解元组
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
        char1 = '{"name": name, "email": name+"@163.com" , "age": age, "sex": sex}'
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
        table.insert_table(table_name, keys, values, dict1)
    # table.insert_tablemany(table_name,list_rows)
    table.drop_table(table_name)
    table.close_link()
    print("写入{DATA}条数据，耗时{time}秒".format(DATA=i, time=time.perf_counter() - start))

