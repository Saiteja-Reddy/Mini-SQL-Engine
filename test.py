import sqlparse
import sys
import string
import glob

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

	info = {}
	for col in cols:
		info[col] = []

	f1 = open(file, 'r')
	for line in f1:
		line = line.split(",")
		line = list(map(str.strip, line))
		for i,col in enumerate(cols):
			info[col].append(line[i])

	table_data[table] = info

print(table_data)
### done reading tables data


## do join operations if any

### handle where

### handle project operations


