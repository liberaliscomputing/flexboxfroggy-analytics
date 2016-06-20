# -*- coding: utf-8 -*-

import os
import simplejson as json

def merge_data(dname):
	data = {}
	for fname in os.listdir(dname):
		path = dname + fname
		imported_json = json.loads(open(path).read())
		data.update(imported_json)

	with open(dname + 'data.json', 'w') as f:
		f.write(json.dumps(data, indent=2))

if __name__ == '__main__':
	dname = 'data/'
	merge_data(dname)

