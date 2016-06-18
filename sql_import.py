# -*- coding: utf-8 -*-

import os
from os.path import join, dirname
from dotenv import load_dotenv
import MySQLdb as mdb
import simplejson as json


def sql_import(dname):
	try:
		print 'Start processing'
		# import environmental variables storing database credentials
		dotenv_path = join(dirname(__file__), '.env')
		load_dotenv(dotenv_path)
		# connect to database
		con = mdb.connect(host=os.environ['hostname'], \
			user=os.environ['username'], \
			passwd=os.environ['password'], \
			db=os.environ['database'])
		if con:
			print 'Connected to MySQL database'
		# get cursor
		cur = con.cursor()
		# load and import json
		for fname in os.listdir(dname):
			path = dname + fname
			print 'Load %s' % fname
			imported_json = json.loads(open(path).read())
			print 'Import %s' % fname
			for (k, v) in imported_json.items():
				cur.execute('INSERT INTO log(transaction_key, user, timestamp, level_name, input, result) \
					VALUES("%s", "%s", "%s", "%s", "%s", "%s")' % \
					(k, v['user'], str(v['timeStamp']), v['levelName'], v['input'], v['result']))
				con.commit()

	except mdb.Error, e:
		print 'Error %d: %s' % (e.args[0], e.args[1])
		exit()

	finally:
		if con:
			print 'Close session'
			con.close()

if __name__ == '__main__':
	dname = 'data/'
	sql_import(dname)

