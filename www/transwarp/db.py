#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
		