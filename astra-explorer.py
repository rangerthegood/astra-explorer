import requests
import json
import time
import os


class AstraApi:
	TYPES = ["ascii","text","varchar","tinyint","smallint","int","bigint","varint","decimal","float","double","date","DateRangeType","duration","time","timestamp","uuid","timeuuid","blob","boolean","counter","inet","PointType","LineStringType","PolygonType","frozen","list","map","set","tuple"]

	def __init__(self, databaseid, region, username, password):
		self._auth_token = None
		self.last_update = None
		self.databaseid = databaseid
		self.region = region
		self.username = username
		self.password = password
		self.keyspace = None
		self.table = None

	def auth(self):
		url = "https://{databaseid}-{region}.apps.astra.datastax.com/api/rest/v1/auth".format(databaseid=self.databaseid, region=self.region)

		headers = {
		    "accept": "*/*",
		    "content-type": "application/json"
		}

		payload = {
		    "username": self.username,
		    "password": self.password
		}

		response = requests.request("POST", url, json=payload, headers=headers)
		if response.status_code == 201:
			data = json.loads(response.text)
			self._auth_token = data['authToken']
			self.last_update = time.time()

			return self._auth_token
		else:
			print("Failed to get auth token")
		
	def get_auth_token(self):
		if self._auth_token == None:
			return self.auth()
		else:
			cur_time = time.time()
			if cur_time > self.last_update * 60 * 30:
				# expired token
				return self.auth()
		return self._auth_token
		
		

	def get_keyspaces(self):
		self._auth_token = self.get_auth_token()

		url = "https://{databaseId}-{region}.apps.astra.datastax.com/api/rest/v1/keyspaces".format(databaseId=self.databaseid, region=self.region)

		headers = {
		    "accept": "application/json",
		    "x-cassandra-token": self._auth_token,
		}

		response = requests.request("GET", url, headers=headers)
		if response.status_code != 200:
			self.auth_token = None
			print("Failed to query keyspace, reauthenticate")
			return

		data = json.loads(response.text)
		return data

	def set_keyspace(self):
		data = self.get_keyspaces()

		idx = 1
		print("Keyspaces:")
		for d in data:
			print("%d - %s" % (idx, d))
			idx = idx + 1
		
		
		keyspace_num = int(input("Enter the keyspace number: "))
		keyspace_num = keyspace_num - 1
		if (keyspace_num >= 0 and keyspace_num < len(data)):
			print('Keyspace %s set!' % data[keyspace_num])
			self.keyspace = data[keyspace_num]	
		

	def get_all_tables(self, keyspace):
		self.auth_token = self.get_auth_token()

		url = "https://{databaseId}-{region}.apps.astra.datastax.com/api/rest/v1/keyspaces/{keyspaceName}/tables".format(databaseId=self.databaseid, region=self.region, keyspaceName=keyspace)

		headers = {
		    "accept": "*/*",
		    "x-cassandra-token": self._auth_token
		}

		response = requests.request("GET", url, headers=headers)
		if response.status_code != 200:
			self.auth_token = None
			print("Failed to query keyspace, reauthenticate")
			return

		data = json.loads(response.text)
		return data

	def set_table(self):
		if self.keyspace == None:
			print("Keyspace not set")
			return

		data = self.get_all_tables(self.keyspace)

		idx = 1
		print("Tables:")
		for d in data:
			print("%d - %s" % (idx, d))
			idx = idx + 1

		table_num = int(input("Enter the table number: "))
		table_num = table_num - 1
		if (table_num >= 0 and table_num < len(data)):
			print('Table %s set!' % data[table_num])
			self.table = data[table_num]	

			self.get_table(self.keyspace, self.table)
		

	def get_table(self, keyspace, table):
		self._auth_token = self.get_auth_token()

		url = "https://{databaseId}-{region}.apps.astra.datastax.com/api/rest/v1/keyspaces/{keyspaceName}/tables/{tableName}".format(databaseId=self.databaseid, region=self.region, keyspaceName=keyspace, tableName=table)

		headers = {
		    "accept": "*/*",
		    "x-cassandra-token": self._auth_token
		}


		response = requests.request("GET", url, headers=headers)
		if response.status_code != 200:
			self.auth_token = None
			print("Failed to query keyspace, reauthenticate")
			return

		print(response.text)
		data = json.loads(response.text)
		print('Table %s.%s' % (keyspace, table))
		if 'columnDefinitions' in data:
			cols = data['columnDefinitions']
			for c in cols:
				print('\t%s' % c)

	def get_type_for_column(self):
		while True:
			idx = int(input("Enter type number, (-1 for list): "))
			if idx == -1:
				self.display_types()	
			else:
				name = self.get_typename(idx)
				return name
			

	def create_table(self):
		self._auth_token = self.get_auth_token()

		if self.keyspace == None:
			print("A keyspace is not selected")
			return
		
		print("Creating new table in keyspace: %s" % self.keyspace)

		table_name = input("Table Name: ")
		
		print("Gathering information for new table %s.%s" % (self.keyspace, table_name))

		first_col = None

		col_list = []
		while True:
			col_name = input("Enter colunmname (-1 if done): ")
			if col_name == '-1':
				#done
				break
			if first_col == None:
				first_col = col_name
			
			col_type = self.get_type_for_column()

			new_pair = {}
			new_pair['name'] = col_name
			new_pair['typeDefinition'] = col_type
			new_pair['static'] = False
			col_list.append(new_pair)
			
			
		url = "https://{databaseId}-{region}.apps.astra.datastax.com/api/rest/v1/keyspaces/{keyspaceName}/tables".format(databaseId=self.databaseid, region=self.region, keyspaceName=self.keyspace)
		payload = {
		    "ifNotExists": False,
		    "columnDefinitions": col_list,
		    "primaryKey": {"partitionKey": [first_col]},
		    "tableOptions": {"defaultTimeToLive": 0},
		    "name" : table_name

		}
		print(payload)
		headers = {
		    "accept": "*/*",
		    "content-type": "application/json",
		    "x-cassandra-token": self._auth_token
		}

		response = requests.request("POST", url, json=payload, headers=headers)
		if response.status_code == 201:
			print(response.text)
		else:
			print("Failed to create new table")
			print(response.text)


	def get_all_rows(self):
		self.auth_token = self.get_auth_token()

		if self.keyspace == None:
			print("Keyspace not set")
			return
		
		if self.table == None:
			print("Table not set");
			return

		url = "https://{databaseId}-{region}.apps.astra.datastax.com/api/rest/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows".format(databaseId=self.databaseid, region=self.region, keyspaceName=self.keyspace, tableName=self.table)

		headers = {
		    "accept": "application/json",
		    "x-cassandra-token": self._auth_token
		}

		response = requests.request("GET", url, headers=headers)
		if response.status_code != 200:
			self.auth_token = None
			print("Failed to query keyspace, reauthenticate")
			return

		data = json.loads(response.text)
		if 'rows' not in data:
			print("No rows to display")
			return
	
		idx = 1
		print('Results for %s.%s' % (self.keyspace, self.table))
		for d in data['rows']:
			print('\t%d - %s' % (idx, d))
			idx = idx + 1

	def get_table_columns(self):
		self.auth_token = self.get_auth_token()

		if self.keyspace == None:
			print("Keyspace is not set")
			return

		if self.table == None:
			print("Table is not set")
			return

		url = "https://{databaseId}-{region}.apps.astra.datastax.com/api/rest/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns".format(databaseId=self.databaseid, region=self.region, keyspaceName=self.keyspace, tableName=self.table)

		headers = {
		    "accept": "application/json",
		    "x-cassandra-token": self._auth_token
		}

		response = requests.request("GET", url, headers=headers)
		if response.status_code != 200:
			self.auth_token = None
			print("Failed to query keyspace, reauthenticate")
			return

		print("Keyspace: %s Table %s" % (self.keyspace, self.table))

		data = json.loads(response.text)
		for d in data:
			print("\tName: %s, Type: %s" % (d['name'], d['typeDefinition']))
		
		return data

	def display_types(self):
		idx = 1
		for t in self.TYPES:
			print("%d - %s" % (idx, t))
			idx = idx + 1

	def get_typename(self, idx):
		return self.TYPES[idx-1]


def menu():
	print("")
	print("1 - Set keyspace")
	print("2 - Set table")
	print("3 - Get table columns")
	print("4 - Get all rows")
	print("5 - Create Table")
	print("6 - Exit")

	idx = input("> ")
	print("")

	return idx
		


def main():
	cluster_id = os.getenv('ASTRA_CLUSTER_ID', None)
	if cluster_id == None:
		print("ASTRA_CLUSTER_ID ENV VAR Missing")
		return

	region = os.getenv('ASTRA_CLUSTER_REGION', None)
	if region == None:
		print("ASTRA_CLUSTER_REGION ENV VAR Missing")
		return

	username = os.getenv('ASTRA_DB_USERNAME', None)
	if username == None:
		print("ASTRA_DB_USERNAME ENV VAR Missing")
		return

	password = os.getenv('ASTRA_DB_PASSWORD', None)
	if password == None:
		print("ASTRA_DB_PASSWORD ENV VAR Missing")
		return
	

	astra = AstraApi(cluster_id, region, username, password)

	cmd = menu()
	while True:
		if cmd == '1':
			astra.set_keyspace()	
		elif cmd == '2':
			astra.set_table()	
		elif cmd == '3':
			astra.get_table_columns()	
		elif cmd == '4':
			astra.get_all_rows()
		elif cmd == '5':
			astra.create_table()
		elif cmd == '6':
			break

		# get next command
		cmd = menu()

if __name__ == '__main__':
	main()
