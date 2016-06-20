# -*- coding: utf-8 -*-

from os.path import join, dirname
from dotenv import load_dotenv
import os
import MySQLdb as mdb
import ujson as json

def sql_import(dname):
	try:
		print 'Start processing'	
		# import environmental variables storing database credentials
		dotenv_path = join(dirname(__file__), '.env')
		load_dotenv(dotenv_path)
		# connect to database
		con = (mdb.connect(host=os.environ['hostname'], 
			user=os.environ['username'], 
			passwd=os.environ['password'], 
			db=os.environ['database'], 
			use_unicode=True, charset='utf8mb4'))
		if con:
			print 'Connected to MySQL database'	
		# get cursor
		cur = con.cursor()
		# load and import json	
		for fname in os.listdir(dname):
			print 'Load %s' % fname
			path = dname + fname	
			loaded_json = json.loads(open(path).read())
			print 'Import %s' % fname
			errcnt = 0
			for (k, v) in loaded_json.items():
				try:
					query = ('INSERT INTO log(transaction_key, user, timestamp, level_name, changed, input, result)' + 
						'VALUES("%s","%s", "%s", "%s", "%s", "%s", "%s")')
					# replace endswith \\, ', and " for fewer exceptions
					cur.execute((query % 
						(k, v['user'], str(v['timeStamp']), v['levelName'], v['changed'], v['input'], v['result'])))
				except Exception as err:
					errcnt += 1
					print 'Error: %s in importing %s\n' % (err, v)
					pass
			con.commit()
			print 'Find %d error(s) in importing %s\n' % (errcnt, fname)
	finally:
		if con:
			print 'Close connection'
			con.close()

if __name__ == '__main__':
	dname = 'data/'
	sql_import(dname)

