import sqlparse
import sys
import string
import glob
import re
import itertools

# print(sys.argv)

sql = sys.argv[1]

print("Input SQL is :", sql)

parsed = sqlparse.parse(sql)

stmt = parsed[0]

# print(stmt.get_type())

from_flag = 0
selections = []
tables = ""
where = ""

for i,token in enumerate(stmt.tokens):

	if(i is 0):
		print("Type of Statement: ", token)
		continue
	
	if(str(token.ttype) == "Token.Text.Whitespace" or str(token.ttype) == "Token.Punctuation" ):
		continue

	if(type(token) is sqlparse.sql.Where):
		where = token
		print("Where:", where)
		continue		

	if(from_flag == 1):
		tables = str(token)
		print("Tables: ", tables)
		from_flag = 2
		continue

	if(str(token) == "from"):
		from_flag += 1
		continue

	selections.append(str(token))
		
	# print(str(token), str(Type(token)), token.ttype)

print(selections, tables, where)


tables = tables.split(",")
tables = list(map(str.strip, tables))
print(tables)

table_info = {}
flag = 0
name = ""
step = -1
columns = []

f = open('metadata.txt')
for line in f:
	line = line.strip()
	# print(line)
	if line == "<begin_table>":
		flag = 1
		step = 1
		continue
	
	if step == 1:
		name = line
		step += 1
		continue

	if line == "<end_table>":
		flag = 0
		step = -1
		table_info[name] = columns
		columns = []
		continue

	columns.append(line)

f.close()

print(table_info)

avail_met = list(table_info.keys())

avail_files = glob.glob("*.csv")

table_data = {}

for table in tables:
	file = table + ".csv"
	if(table not in avail_met):
		print("Metadata not available for Table : ", table)
		exit()

	if(file not in avail_files):
		print("Table File not available for Table : ", table)
		exit()

	# print(file)

	cols = table_info[table]

	# info = {}
	# for col in cols:
		# info[col] = []

	table_data[table] = []

	f1 = open(file, 'r')
	for line in f1:
		line = line.split(",")
		line = list(map(str.strip, line))
		if len(line) != len(cols):
			print("Data in table :" + table, "not matching metadata.")
			exit()
		table_data[table].append(line)
		# for i,col in enumerate(cols):
			# info[col].append(line[i])

	# table_data[table] = info

print(table_data)
### done reading tables data


## do join operations if any
print("Do Join:")
inp = [table_data[tab] for tab in tables]
out = list(itertools.product(*inp))
joined = [list(itertools.chain(*a)) for a  in out]
print(joined)
print(len(joined))

tab_inf = [[ tab+'.'+a for a  in table_info[tab]] for tab in tables]
tab_inf = list(itertools.chain(*tab_inf))
print(tab_inf)

### handle where
print(where)
where = str(where).strip()
where = where.strip('where')
where = where.rstrip(';')
where = where.strip()
print("where:", where)

print(re.split('(<=|>=|<|>|=)', where))

### handle project operations


