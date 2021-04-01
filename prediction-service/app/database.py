import sqlalchemy


class Database:
	def __init__(self, db_conf):
		"""
		Setup DB connection
		"""

		# Build connection URL
		dburl = 'postgresql://{}:{}@{}:{}/{}'.format(
			db_conf['username'],
			db_conf['password'],
			db_conf['host'],
			db_conf['port'],
			db_conf['db_name']
		)

		# Create DB connection
		self.__engine = sqlalchemy.create_engine(dburl, client_encoding='utf8', pool_size=15, max_overflow=5)

		# Build metadata
		self.__meta = sqlalchemy.MetaData(bind=self.__engine, reflect=True)

		# Build table dict
		self.__table_lookup = dict()

	def teardown(self):
		"""
		Dispose connection pool in the engine
		"""

		self.__engine.dispose()

	def get_table(self, table_name):
		"""
		Get the table object from metadata, and put in local cache
		"""

		if table_name not in self.__table_lookup:
			self.__table_lookup[table_name] = self.__meta.tables[table_name]

		return self.__meta.tables[table_name]

	def insert(self, table_name, values):
		"""
		Insert into the table using the cached table object.
		Use a connection from the pool to insert, then return it back.
		"""

		insert_clause = self.get_table(table_name).insert().values(values)
		with self.__engine.connect() as conn:
			conn.execute(insert_clause)
