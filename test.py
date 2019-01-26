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

def find_var(label, table_info, tables, tab_inf):
	if label.find('.') != -1:
		# print("Search in tab_inf")
		for i,lab in enumerate(tab_inf):
			if label == lab:
				return (label, i)
	else:
		for tab in tables:
			for dat in table_info[tab]:
				if dat == label:
					curr = tab + "." + dat
					for i,lab in enumerate(tab_inf):
						if curr == lab:
							return (curr, i)					

	print("Could not find variable " + label)
	exit()	

def run_where_op(where, joined_data, table_info, tables, tab_inf):
	data = []
	data_bin = []
	print("here: " , where)
	where = re.split('(<=|>=|<|>|=)', where)
	where = [a.strip() for a in where]
	# print(where)
	where[0] = find_var(where[0], table_info, tables, tab_inf)
	if where[2].isdigit():
		print("Relational OP")
		if(where[1] == "="):
			where[1] = "=="
		# print(find_var(where[0], table_info, tables))
		op = "dat[" + str(where[0][1]) + "] " + str(where[1]) + " " + str(where[2])
		# print(op)
		for dat in joined_data:
			if eval(op):
				data.append(dat)
				data_bin.append(1)
			else:
				data_bin.append(0)
		# print(data)
	else:
		where[2] = find_var(where[2], table_info, tables, tab_inf)
		if where[1] == "=":
			print("Join OP") ### modify this to remove column
			where[1] = "=="
			op = "dat[" + str(where[0][1]) + "] " + str(where[1]) + " dat[" + str(where[2][1]) + "]"
			# print(op)
			for dat in joined_data:
				if eval(op):
					# del dat[where[0][1]]
					data.append(dat)
					data_bin.append(1)
				else:
					data_bin.append(0)						
			# del tab_inf[where[0][1]]

		else:
			print("Join Cmp OP")	
			op = "dat[" + str(where[0][1]) + "] " + str(where[1]) + " dat[" + str(where[2][1]) + "]"
			# print(op)
			for dat in joined_data:
				if eval(op):
					# del dat[where[0][1]]
					data.append(dat)
					data_bin.append(1)
				else:
					data_bin.append(0)						
			# del tab_inf[where[0][1]]			

	return (data, tab_inf, data_bin)

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

# print(table_info)

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
		line = list(map(int, line))
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
# print(len(joined))

tab_inf = [[ tab+'.'+a for a  in table_info[tab]] for tab in tables]
# tab_inf = [[ a for a  in table_info[tab]] for tab in tables]
tab_inf = list(itertools.chain(*tab_inf))
print(tab_inf)

### handle where
# print(where)
where = str(where).strip()
where = where.strip('where')
where = where.rstrip(';')
where = where.strip()
# print("where:", where)

# data = []
where_data_fin = joined

if(where != ""):
	where_data_fin = []
	if where.find(" AND ") is not -1:
		options = where.split(" AND ")
		where_dat_1, where_tab_inf_1, where_dat_bin_1 = run_where_op(options[0], joined, table_info, tables, tab_inf)
		# print(where_dat_1, where_tab_inf_1, where_dat_bin_1)

		where_dat_2, where_tab_inf_2, where_dat_bin_2 = run_where_op(options[1], joined, table_info, tables, tab_inf)
		# print(where_dat_2, where_tab_inf_2, where_dat_bin_2)	

		fin = [where_dat_bin_1[i] & where_dat_bin_2[i] for i in range(0, len(where_dat_bin_1)) ]


		for i,f in enumerate(fin):
			if f:
				where_data_fin.append(joined[i])
		# print(where_data_fin)
	elif where.find(" OR ") is not -1:
		options = where.split(" OR ")
		where_dat_1, where_tab_inf_1, where_dat_bin_1 = run_where_op(options[0], joined, table_info, tables, tab_inf)
		# print(where_dat_1, where_tab_inf_1, where_dat_bin_1)

		where_dat_2, where_tab_inf_2, where_dat_bin_2 = run_where_op(options[1], joined, table_info, tables, tab_inf)
		# print(where_dat_2, where_tab_inf_2, where_dat_bin_2)	

		fin = [where_dat_bin_1[i] | where_dat_bin_2[i] for i in range(0, len(where_dat_bin_1)) ]

		for i,f in enumerate(fin):
			if f:
				where_data_fin.append(joined[i])

		# print(where_data_fin)
	else:
		where_dat, where_tab_inf, where_dat_bin = run_where_op(where, joined, table_info, tables, tab_inf)
		# print(where_dat, where_tab_inf, where_dat_bin)
		where_data_fin = where_dat


print(where_data_fin, tab_inf)

### handle project operations
print()
print()
print()

selections = selections[0].split(",")
selections = list(map(str.strip, selections))
print(selections)

if "*" in selections:
	# print("project ALL")
	for lab in tab_inf:
		print(lab, end=",")
	print()
	for entry in where_data_fin:
		print(entry)
else:
	indices = []
	for sel in selections:
		if sel != "*":
			now = find_var(sel, table_info, tables, tab_inf)
			indices.append(now[1])
			# print(now)
	for i in indices:
		print(tab_inf[i], end=",")
	print()	
	for entry in where_data_fin:
		for i in indices:
			print(entry[i], end=",")
		print()



