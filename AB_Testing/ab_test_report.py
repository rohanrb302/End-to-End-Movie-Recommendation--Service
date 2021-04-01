import argparse
import json

import pandas as pd
import sqlalchemy


def connect_db(cfg_path):
	'''
	Setup DB connection
	'''

	with open(cfg_path) as cfg:
		dbConf = json.load(cfg)

	# Build connection URL
	dburl = 'postgresql://{}:{}@{}:{}/{}'.format(
		dbConf['username'],
		dbConf['password'],
		dbConf['host'],
		dbConf['port'],
		dbConf['db_name']
	)

	return sqlalchemy.create_engine(dburl, client_encoding='utf8')


def report_model_online_evaluation(model_id):
	"""
	model id for the main model is usually 0
	model id for the test model is usually 1
	return: float in [0,1], represent the persentage of users who are satisfied with the recommendation
	"""

	# connect to db
	engine = connect_db('config.json')

	# select rows with specified model id
	with engine.connect() as con:
		df = pd.read_sql_query('SELECT * FROM recommendations WHERE model_id=\'%s\';' % model_id, con=con)

	engine.dispose()

	# How many user are guide to this model#
	num_total = len(df.index)
	print("{0} user are guided to the {1}th model".format(num_total, model_id))

	# How many of them like the recommendation
	num_like = df[df.rating == 1].shape[0]
	perc_like = float(num_like) / float(num_total)
	print("{0} % user like the recommendation".format(perc_like * 100))

	return perc_like


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='given model id, generate report')
	parser.add_argument('--modelid', type=str,
						help='input the model id, usally a timestamp form')
	args = parser.parse_args()
	perc_like = report_model_online_evaluation(model_id=args.modelid)
