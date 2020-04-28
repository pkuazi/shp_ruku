# coding: utf-8
'''
Created on Apr 7, 3
Created on Apr 7, 3

@author: root
'''  
from psycopg2 import  extras
import psycopg2 


# from MySQLdb import cursors
# yum install python-psycopg2
class PqWarper(object):
    def __init__(self, _conn):
        self._conn = _conn
        self.cursor = None
        
    def get_cursor(self):
        self.cursor = self._conn.cursor(cursor_factory=extras.DictCursor)
        return self.cursor        
        
    def __enter__(self):  
        return self.get_cursor() 
    
    def __exit__(self, exc, value, tb):
        if self.cursor :
            self.cursor.close()         
        if exc:
            self._conn.rollback()
        else:
            self._conn.commit()        
        
class Pgsql(object):    

    def __init__(self, host, user, passwd, db, charset="utf8"): 
#         self._conn = MySQLdb.connect(host, user, passwd, db, charset=charset, cursorclass=cursors.DictCursor)
        self._conn = psycopg2.connect(host=host, user=user, password=passwd, database=db)

    def __del__(self):
        self._conn.close()  
        
    def getConn(self):
        return PqWarper(self._conn) 
 
    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/None 查询到的结果集
        """ 
        with self.getConn() as c:            
            if param is None:
                c.execute(sql)
            else:
                c.execute(sql, param)
            return c.fetchall()

    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/None 查询到的结果集
        """ 
        with self.getConn() as c:      
            if param is None:
                c.execute(sql)
            else:
                c.execute(sql, param)
            return c.fetchone()
        
    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/None 查询到的结果集
        """ 
        with self.getConn() as c:      
            if param is None:
                c.execute(sql)
            else:
                c.execute(sql, param)
            return c.fetchmany(num)

    def insertOne(self, sql, value):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """ 
        with self.getConn() as c:      
            c.execute(sql, value)        
#             c.execute("SELECT @@IDENTITY AS id")	
#             return c.fetchone()[0] 
#         return 1

    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """ 
        with self.getConn() as c:      
            return c.executemany(sql, values)

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """ 
        with self.getConn() as c:      
            if param is None:
                return c.execute(sql)
            else:
                return c.execute(sql, param) 

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """ 
        with self.getConn() as c:      
            if param is None:
                return c.execute(sql)
            else:
                return c.execute(sql, param) 

#     def begin(self):
#         """
#         @summary: 开启事务
#         """
# #         self._conn.begin()  # .autocommit(0) 
#         pass 
# 
#     def commit(self,):
#         """
#         @summary: 结束事务
#         """
#         self._conn.commit()
#         
#     def rollback(self,):  
#         """
#         @summary: 回滚事务
#         """
#         self._conn.rollback()  
   
    
if __name__ == "__main__":
    m = Pgsql("127.0.0.1", "postgres", "", "eagledb")
    #m.insertOne("insert into node_tasks (taskid, taskinfo, node, ctime) values (%s, %s, %s, %s)", ["test1", "{}", "127.0.0.1", datetime.datetime.now()])
    print( m.getAll("select count(1) from node_tasks", param=None))
    pass
