import sqlparse
import sys
import string
import glob
import re
import itertools
import numpy as np
from copy import copy
from statistics import mean

if len(sys.argv) != 2:
	print("Error: Incorrect usage!!")
	print("Correct Usage: python 201564086.py \"<sql>\"")
	exit()

sql = sys.argv[1]

sql = sql.strip()
if(sql[-1] != ";"):
	print("Error: Need to terminate statement with a semi-colon!!")
	exit()

sql = sql[:-1]

parsed = sqlparse.parse(sql)

stmt = parsed[0]

delete_col = ""

def find_var(label, table_info, tables, tab_inf):
	if label.find('.') != -1:
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

	print("Error: Could not find variable " + label)
	exit()	

def run_where_op(where, joined_data, table_info, tables, tab_inf):
	global delete_col
	data = []
	data_bin = []
	where = re.split('(<=|>=|<|>|=)', where)
	where = [a.strip() for a in where]
	if(len(where) is not 3):
		print("Error: Give correct where conditions: <var_one> <op> <var_two>")
		exit()
	where[0] = find_var(where[0], table_info, tables, tab_inf)
	if where[2].isdigit():
		if(where[1] == "="):
			where[1] = "=="
		op = "dat[" + str(where[0][1]) + "] " + str(where[1]) + " " + str(where[2])
		for dat in joined_data:
			if eval(op):
				data.append(dat)
				data_bin.append(1)
			else:
				data_bin.append(0)
	else:
		where[2] = find_var(where[2], table_info, tables, tab_inf)
		if where[1] == "=":
			where[1] = "=="
			op = "dat[" + str(where[0][1]) + "] " + str(where[1]) + " dat[" + str(where[2][1]) + "]"
			for dat in joined_data:
				if eval(op):
					delete_col = where
					data.append(dat)
					data_bin.append(1)
				else:
					data_bin.append(0)						

		else:
			op = "dat[" + str(where[0][1]) + "] " + str(where[1]) + " dat[" + str(where[2][1]) + "]"
			for dat in joined_data:
				if eval(op):
					data.append(dat)
					data_bin.append(1)
				else:
					data_bin.append(0)						

	return (data, tab_inf, data_bin)

from_flag = 0
selections = []
tables = ""
where = ""

for i,token in enumerate(stmt.tokens):

	if(i is 0):
		continue
	
	if(str(token.ttype) == "Token.Text.Whitespace" or str(token.ttype) == "Token.Punctuation" ):
		continue

	if(type(token) is sqlparse.sql.Where):
		where = token
		continue		

	if(from_flag == 1):
		tables = str(token)
		from_flag = 2
		continue

	if(str(token) == "from"):
		from_flag += 1
		continue

	selections.append(str(token))
		

tables = tables.split(",")
tables = list(map(str.strip, tables))

if(tables[0] == ""):
	print("Error: No table given as input!!")
	exit()

table_info = {}
flag = 0
name = ""
step = -1
columns = []

f = open('metadata.txt')
for line in f:
	line = line.strip()
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

avail_met = list(table_info.keys())

avail_files = glob.glob("*.csv")

table_data = {}

for table in tables:
	file = table + ".csv"
	if(table not in avail_met):
		print("Error: Metadata not available for Table : ", table)
		exit()

	if(file not in avail_files):
		print("Error: Table File not available for Table : ", table)
		exit()

	cols = table_info[table]

	table_data[table] = []

	f1 = open(file, 'r')
	for line in f1:
		line = line.split(",")
		line = list(map(str.strip, line))
		line = [a.replace('"', '') for a in line]
		line = list(map(int, line))
		if len(line) != len(cols):
			print("Error: Data in table :" + table, "not matching metadata.")
			exit()
		table_data[table].append(line)


### done reading tables data


## do join operations if any

inp = [table_data[tab] for tab in tables]
out = list(itertools.product(*inp))
joined = [list(itertools.chain(*a)) for a  in out]


tab_inf = [[ tab+'.'+a for a  in table_info[tab]] for tab in tables]

tab_inf = list(itertools.chain(*tab_inf))

### handle where

where = str(where).strip()
if(where == "where"):
	print("Error: No where condition specfied!!")
	exit()

where = where.strip('where')
where = where.rstrip(';')
where = where.strip()

where_data_fin = joined

where_tab_inf = tab_inf

if(where != ""):
	where_data_fin = []
	if where.find(" AND ") is not -1:
		options = where.split(" AND ")
		where_dat_1, where_tab_inf_1, where_dat_bin_1 = run_where_op(options[0], joined, table_info, tables, tab_inf)

		where_dat_2, where_tab_inf_2, where_dat_bin_2 = run_where_op(options[1], joined, table_info, tables, tab_inf)

		fin = [where_dat_bin_1[i] & where_dat_bin_2[i] for i in range(0, len(where_dat_bin_1)) ]

		where_tab_inf = where_tab_inf_1

		for i,f in enumerate(fin):
			if f:
				where_data_fin.append(joined[i])

	elif where.find(" OR ") is not -1:
		options = where.split(" OR ")
		where_dat_1, where_tab_inf_1, where_dat_bin_1 = run_where_op(options[0], joined, table_info, tables, tab_inf)

		where_dat_2, where_tab_inf_2, where_dat_bin_2 = run_where_op(options[1], joined, table_info, tables, tab_inf)

		fin = [where_dat_bin_1[i] | where_dat_bin_2[i] for i in range(0, len(where_dat_bin_1)) ]

		for i,f in enumerate(fin):
			if f:
				where_data_fin.append(joined[i])

		where_tab_inf = where_tab_inf_1

	else:
		where_dat, where_tab_inf, where_dat_bin = run_where_op(where, joined, table_info, tables, tab_inf)
		where_data_fin = where_dat


tab_inf = where_tab_inf

### handle project operations

distinct_flag = 0
if selections[0] == "distinct":
	distinct_flag = 1
	del selections[0]

selections = selections[0].split(",")
selections = list(map(str.strip, selections))

find_max = re.compile("max\((.*)\)")
find_min = re.compile("min\((.*)\)")
find_mean = re.compile("average\((.*)\)")
find_sum = re.compile("sum\((.*)\)")

def check_aggregate(sel):
	 out_max = find_max.match(sel) is not None
	 out_min = find_min.match(sel) is not None
	 out_mean = find_mean.match(sel) is not None
	 out_sum = find_sum.match(sel) is not None
	 return out_max | out_mean | out_min | out_sum

def get_aggregate(sel):
	 out_max = find_max.match(sel) is not None
	 if out_max:
	 	return ('max',  find_max.match(sel).groups()[0])
	 out_min = find_min.match(sel) is not None
	 if out_min:
	 	return ('min',  find_min.match(sel).groups()[0])	 
	 out_mean = find_mean.match(sel) is not None
	 if out_mean:
	 	return ('mean',  find_mean.match(sel).groups()[0])		 
	 out_sum = find_sum.match(sel) is not None
	 if out_sum:
	 	return ('sum',  find_sum.match(sel).groups()[0])
	 return ('not', -1)


out_lab = []
output_final = []

if "*" in selections:
	out_lab = tab_inf
	output_final = where_data_fin
else:
	indices = []
	agg_f = 0
	for sel in selections:
			if check_aggregate(sel):
				agg_f = 1
				break

	if agg_f == 0:
		for sel in selections:
			if sel != "*":
				now = find_var(sel, table_info, tables, tab_inf)
				indices.append(now[1])

		for i in indices:
			out_lab.append(tab_inf[i])
		for entry in where_data_fin:
			cur = []
			for i in indices:
				cur.append(entry[i])
			output_final.append(cur)

	else:
		for sel in selections:
				if not check_aggregate(sel):
					print("Error: Cannot combine non-aggregate with aggregate projection.")
					exit()

		p_data = copy(where_data_fin)
		names = []
		output = []	

		for sel in selections:
			now = get_aggregate(sel)
			if now[0] == "min":
				id_n = find_var(now[1], table_info, tables, tab_inf)
				names.append("min(" + id_n[0] + ")")
			if now[0] == "max":
				id_n = find_var(now[1], table_info, tables, tab_inf)
				names.append("max(" + id_n[0] + ")")
			if now[0] == "mean":
				id_n = find_var(now[1], table_info, tables, tab_inf)
				names.append("average(" + id_n[0] + ")")
			if now[0] == "sum":
				id_n = find_var(now[1], table_info, tables, tab_inf)
				names.append("sum(" + id_n[0] + ")")

		if(len(p_data) != 0):
			for sel in selections:
				now = get_aggregate(sel)
				if now[0] == "min":
					id_n = find_var(now[1], table_info, tables, tab_inf)
					temp = [min([p_data[d][b] for d in range(len(p_data))]) for b in range(len(p_data[0]))]
					output.append(temp[id_n[1]])
				if now[0] == "max":
					id_n = find_var(now[1], table_info, tables, tab_inf)
					temp = [max([p_data[d][b] for d in range(len(p_data))]) for b in range(len(p_data[0]))]
					output.append(temp[id_n[1]])
				if now[0] == "mean":
					id_n = find_var(now[1], table_info, tables, tab_inf)
					temp = [mean([p_data[d][b] for d in range(len(p_data))]) for b in range(len(p_data[0]))]
					output.append(temp[id_n[1]])
				if now[0] == "sum":
					id_n = find_var(now[1], table_info, tables, tab_inf)
					temp = [sum([p_data[d][b] for d in range(len(p_data))]) for b in range(len(p_data[0]))]
					output.append(temp[id_n[1]])

		out_lab = names
		output_final.append(output)



test_lab = copy(out_lab)


if delete_col != "" and "*" in selections:
	index = delete_col[2][1] 		

	if(index != -1):
		for dat in output_final:
			del dat[index]

		del out_lab[index]

out_lab = list(map(str, out_lab))
out_lab = ",".join(out_lab)
print(out_lab)
if(distinct_flag):
	output_final = [list(x) for x in set(tuple(x) for x in output_final)]
for entry in output_final:
	entry = list(map(str, entry))
	entry = ",".join(entry)
	print(entry)

