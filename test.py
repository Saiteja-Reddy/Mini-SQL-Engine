import sqlparse
import sys

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
		tables = token
		print("Tables: ", tables)
		from_flag = 2
		continue

	if(str(token) == "from"):
		from_flag += 1
		continue

	selections.append(str(token))
		
	# print(str(token), str(Type(token)), token.ttype)

print(selections, tables, where)