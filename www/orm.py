#!/usr/bin/env python3
#-*-coding:utf-8-*-

''
__author__='junjun'

import logging
import asyncio
import aiomysql

def log(sql,args=()):
	logging.info("SQL:%s"% sql)

#创建全局数据库连接池，使得每个http请求都能从连接池中直接获取数据库连接
#避免了频繁的打开或者关闭数据库连接

@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info("create database connection pool...")
	global __pool
	#调用一个子协程来创建全局连接池，create_pool的返回值是一个pool实例对象
	__pool=yield from aiomysql.create_pool(
		host = kw.get("host","localhost"),#数据库服务器的位置，设在本地
		port  = kw.get("port",3306),#mysql的端口
		user  = kw["user"],#登录用户名
		password=kw["password"],#口令
		db= kw['database'],#当前数据库名
		charset = kw.get("charset","utf-8"),#设置连接使用的编码格式为utf_8
		autocommit=kw.get("autocommit",True),#自动提交模式，默认是False

		maxsize = kw.get("maxsize",10),
		#最大连接池的大小，此处设为10
		minsize = kw.get("minsize",1),
		#最小连接池大小，默认是10，此处设为1，保证任何时候都有一个数据库连接
		loop = loop# 设置消息循环？？？？
		)


	#将数据库的select操作封装在select函数中去
	#SQL形参就是SQL语句，args表示填入SQL的选项值
	#size用于指定最大的查询数量，不指定将返回所有查询结果


@asyncio.coroutine
def select(sql,args,size=None):
	logging.info("SQL:%s"%sql)
	global __pool
	#从连接池中获取一条数据库连接
	with (yield from __pool) as  conn:
		#打开一个DictCursor,它与普通游标的不同在于，以dict形式返回结果
		cur = yield from conn.cursor(aiomysql.DictCursor)
		#sql语句的占位符为？，MySQL的占位符为%s，因此需要进行替换
		#若没有指定args，将使用默认的select语句（在Metaclass内定义）进行查询
		yield from cur.execute(sql.replace("?","%s"),args or ())
		if size:
			rs = yield from cur.fetchmany(size)
		else:
			rs = yield from cur.fetchall()
		yield from cur.close()
		logging.info("rows return %s" % len(rs))
		return rs
#增加·删除、修改都是对数据库的修改，因此封装到一个函数中
@asyncio.coroutine
def execute(sql,args):
	log(sql)
	with (yield from __pool) as conn:
		try:
			#此处打开的是一个普通的游标
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace("?","%s"),args)
			affected = cur.rowcount#修改，返回影响的行数
			yield from cur.close()
		except BaseException as e:
			raise
		return affected

#构造占位符
def create_args_string(num):
	L = []
	for n in range(num):
		L.append("?")
	return ','.join(L)


#父域，可被其他域继承
class Field(object):
	#域的初始化，包括属性（列）名，属性（列）的类型，是否主键
	#default参数允许orm自己填入缺省值，因此具体的使用请看具体的类怎么使用
	#例如：User 有一个定义在StringField的id，default就用于存储用户的独立id
	#再比如created_at的default就用于存储创建时间的浮点表示

	def __init__(self,name,column_type,primary_key,default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default
	#用于打印该对象信息，依次为类名（域名），属性类型，属性名
	def __str__(self):
		return "<%s，%s:%s>" % (self.__class__.__name__,self.column_type,self.name)


	#字符串域
class StringField(Field):
	#ddl("data definition languages"),用于定义数据类型
	
	def __init__(self,name=None,primary_key = False,default = None,ddl="varchar(100)"):
		super().__init__(name,ddl,primary_key,default)

#整数域
class IntegerField(Field):

	def __init__(self,name = None,primary_key=False,default = 0):
		super().__init__(name,"bigint",primary_key,default)

#布尔域
class BooleanField(Field):

	def __init__(self,name=None,default=False):
		super().__init__(name,"boolean",False,default)

#浮点数域
class FloatField(Field):
	def __init__(self,name=None,primary_key=False,default=0.0):
		super().__init__(name,"real",primary_key,default)

#文本域
class TextField(Field):
	def __init__(self,name=None,default=None):
		super().__init__(name,"text",False,default)


#这是一个元类，它定义了如何来构造一个类，任何定义了__metaclass__属性或指定了metaclass的都会通过元类的构造方法构造类
#任何继承自Model的类，都会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性

class ModelMetaclass(type):
	def __new__(cls,name,bases,attrs):
		#cls:当前准备参加的类对象，相当于self
		#name：类名，比如User继承自Model，当使用该元类创建User类时，name=User
		#bases：父类的元组
		#attrs：属性（方法）字典，比如User有__table__,—id等等。。就作为attrs的keys
		


		#排除自身Model类，因为Model类主要就是用来被继承的，其不存在与数据表的映射
		if name =="Model":
			return type.__new__(cls,name,bases,attrs)

		#以下是针对“Model”的子类的处理，将被用于子类的创建.metaclass将被隐式的被继承

		#获取表名，若没有定义__table__属性，将类名作为表名，此处注意or的用法
		tableName = attrs.get("__table__",None) or name
		logging.info("found model:%s (table:%s)"%(name,tableName))
		#获取所有的Field和主键名
		mappings = dict()# 用字典来存储类属性和数据库表的列的映射关系
		fields = [] #用于保存除主键外的属性
		primarykey = None #用于保存主键

		#遍历类的属性，找出定义的域（如StringField，字符串域）内值，建立映射关系
		#k是属性名，v是定义域！请看name=StringField（ddl = "varchar(50))"
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info("found mapping:%s ==>%s" % (k,v))
				mappings[k] = v#建立映射关系
				if v.primary_key:
					if primaryKey: #若主键已经存在，又找到一个主键，将报错，每张表有且仅有一个主键
						raise RuntimeError("Duplicate primary key for field:%s "% s)
					primaryKey=k
				else:
					fields.append(k)#将非主键的属性都加入到fields列表中
		if not primaryKey: #没有找到主键也将报错，因为每张表有且仅有一个主键
			raise RuntimeError("Primary Key not found")
		#从类属性中删除已加入映射字典的建，避免重名
		for k in mappings.keys():
			attrs.pop(k)
		#将非主键的属性变形，放入escaped_fields 中，方便增删改查找语句的书写
		escaped_fields = list(map(lambda f:"'%s'" % f,fields))
		attrs["__mappings__"] = mappings#保存属性和列的映射关系
		attrs["__table__"] = tableName #保存表名
		attrs["__primary_key__"] = primaryKey #保存主键
		attrs["__fields__"]  =fields #保存非主键的属性名

		#构造默认的select，insert，update，delete语句，使用？作为占位符
		attrs["__select__"] = "select '%s',%s from '%s'" %(primaryKey,','.join(escaped_fields),tableName)
		#此处利用create_args_string生成若干个？占位符
		#插入数据时，要指定属性名，并对应的填入属性值（）
		attrs["__insert__"]="insert into '%s' (%s,'%s')values (%s)" % (tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
		#通过主键查找到记录并更新
		attrs["__update__"] = "update '%s' set %s where '%s' = ?" % (tableName,','.join(map(lambda f:"'%s'"% (mappings.get(f).name or f),fields)),primaryKey)
		attrs["__delete__"] = "delete from '%s' where '%s' = ?" % (tableName,primaryKey)
		return type.__new__(cls,name,bases,attrs)

#ORM映射基类，继承自dict，通过ModelMetaclass元类来构造类
class Model(dict,metaclass = ModelMetaclass):

	#初始化函数，调用其父类（dict)的方法
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)
	#增加__getattr__方法，使得获取属性更方便，即可通过“a.b"的形式
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' objiect has no attribute '%s'"% key)


		#增加__setattr__方法，使得设置属性更方便，可通过“a.b=c"的形式
	def __setattr__(self,key,value):
		self[key] = value

	#通过键取值，若值不存在，返回None
	def getValue(self,key):
		return getattr(self,key,None)

	#通过键取值，若值不存在，则返回默认值
	#这个函数很fun
	def getValueOrDefault(self,key):
		value = getattr(self,key,None)
		if value is None:
			field = self.__mappings__[key]#field是一个定义域！比如FloatField
			#default这个属性在这里发挥作用了
			if field.default is not None:
				
				value = field.default() #if　callable(field.default) else field.default
				logging.debug("using default value for %s:%s"% (key,str(value)))
				#通过default取得值后再将其作为当前值
				setattr(self,key,value)
		return value
	@classmethod #装饰器将方法定义为类方法
	@asyncio.coroutine
	def find(cls,pk):
		'find object by primary key'
		#我们之前已经将数据库的select操作封装在select函数中，以下select的参数依次就是SQL，args，size
		rs= yield from select("%s where '%s' = ?"%(cls.__select__,cls,primary_key),[pk],1)
		if len(rs) == 0:
			return None
		#**表示关键字参数，。。。
		#注意：我在select函数中打开的是DictCursor，它会以dict形式返回结果
		return cls(**rs[0])
	@classmethod
	@asyncio.coroutine
	def findAll(cls,where=None,args=None,**kw):
		sql = [cls.__select__]
		#我们定义的默认的select语句是通过主键查询的，并不包括where子句
		#因此若指定有where，需要在select语句中追加关键字
		if where:
			sql.append("where")
			sql.append(where)
		if args is None:
			args = []

		orderBy = kw.get("orderBy",None)
		#解释同where，此处orderBy通过关键字参数传入
		if orderBy:
			sql.append("order by")
			sql.append(orderBy)
		#解释同where
		limit = kw.get("limit",None)
		if limit is not None:
			sql.append("limit")
			if isinstance(limit,int):
				sql.append("?")
				args.append(limit)
			elif isinstance(limit,tuple) and len(limit)==2:
				sql.append("?,?")
				args.extend(limit)
			else:
				raise ValueError("Invalid limit value:%s"% str(limit))
			rs = yield from select(' '.join(sql),args)#没有指定size，因此会fetchall
			return [cls(**r) for f in rs]

	@classmethod
	@asyncio.coroutine
	def findNumber(cls,selectField,where=None,args = None):
		sql = ["select %s _num_ from '%s'"% (selectField,cls.__table__)]
		if where:
			sql.append("where")
			sql.append(where)
		rs = yield from select(' '.join(sql),args,1)
		if len(rs)==0:
			return None
		return rs[0]["_num_"]

	@asyncio.coroutine
	def save(self):
		#我们在定义__insert__时，将主键放在了末尾，因为属性与值要一一对应，因此通过append方法将主键加在最后
		args = list(map(self.getValueOrDefault,self.__fields__))
		#使用getValueOrDefault方法可以调用time.time这样的函数来获取值
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = yield from execute(slef.__insert__,args)
		if rows !=1:#插入一条记录，结果影响的条数不等于1，那肯定出错了
			logging.warn("failed to insert recored:affected rows:%s"%rows)
			


	@asyncio.coroutine
	def update(self):
		#像time.time,next_id之类的函数在插入的时候已经调用过了，没有其他需要更新的值，因此调用getValue
		args = list(map(slef.getValue,self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = yield from execute(self.__update__,args)
		if rows!= 1:
			logging.warn("failed to update by primary key:affencted rows %s"% rows)

	@asyncio.coroutine
	def remove(self):
		args = [self.getValue(self.__primary_key__)]#取得主键作为参数
		rows = yield from execute(self.__delete__,args)#调用默认的delete语句
		if rows !=1:
			logging.warn("failed to remove by primary key:affected rows　%s"% rows )




