#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import uuid
import threading
import logging
#Dict object

class Dict(dict):
	'''
	define a Dict type can present dict with '.'
	example here for doctest.

	>>> d1 = Dict() 
	>>> d1['x'] = 100
	>>> d1.x
	100
	>>> d1.y = 200
	>>> d1['y']
	200
	>>> d2 = Dict(a='1',b='2',c='3')
	>>> d2.c
	'3'
	>>> d2['empty']
	Traceback (most recent call last):
		...
	KeyError: 'empty'
	>>> d2.empty
	Traceback (most recent call last):
		...
	KeyError: 'empty'
	>>> d3= Dict(('a','b','c'),(1,2,3))
	>>> d3.a
	1
	>>> d3.b
	2
	>>> d3.c
	3

	'''
	def __init__(self, name=(), value=(), **kw):
		super(Dict, self).__init__(**kw)
		for k, v in zip(name,value):
			self[k] = v
			
	def __getattr__(self,key):
			return self[key]

	def __setattr__(self, key, value):
		self[key] = value
		pass

def next_id( t = None):
	"""docstring for next_id
	genurate uuid with 50  char
	default as using time.time(), unless the arg is provided

	"""
	
	if t is None:
		t = time.time()
		return '%015d%s000'%(int(t*1000), uuid.uuid4().hex)
		
def _profiling(start, sql=''):
	'''log the time for sql function
	>>> import time, logging
	>>> _profiling(time.time(),'abc')
	'''
	t = time.time() - start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s'%(t,sql))
		# print '[PROFILING] [DB] %s: %s'%(t,sql)
	else:
		logging.info('[PROFILING] [DB] %s: %s'%(t,sql))
		# print 'info'+'[PROFILING] [DB] %s: %s'%(t,sql)
	pass

class _LasyConnection(object):
	"""docstring for _LasyConnection
	Creating DB connection, cursor, commit, cleanup and rollback
	"""
	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			connection = engine.connect() #engine need to be defined in the body: engine = mysql.connector
			logging.info('open connection<%s>...'%hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()
	
	def commit(self):
		self.connection.commit()

	def rollback(self):
		self.connection.rollback()

	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None
			logging.info('close connection <%s>'%hex(id(connection)))
			connection.close()

class _Engine(object):
	"""
	Databass engine class"""
	def __init__(self, connect):
		# super(_Engine, self).__init__()
		self.connect = connect
	def connect(self):
		return self._connect()



engine = None

class _DbCtx(threading.local):
	"""docstring for _DbCtx
	持有数据库连接的上下文对象
	_DbCtx 是threading.local 对象
	所以它持有的数据库连接对于每个线程看到的都是不一样的，
	任何线程都无法访问到其他线程持有的数据库连接"""
	def __init__(self):
		# super(_DbCtx, self).__init__()
		self.connection = None
		self.transactions = 0

	def is_int(self):
		return not self.connection is None

	def init(self):
		self.connection = _LasyConnections()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		self.connection.None\

	def cursor(self):
		return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_int():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	return _ConnectionCtx()


class _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_int():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		return self


	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions ==0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except:
			_db_ctx.connection.rollback()
			raise

	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()
		

if __name__ == '__main__':
	import doctest
	doctest.testmod()