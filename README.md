## Mini-SQL Engine

* Developed a Mini​ Sql Engine which will run a subset of SQL queries using ​command line interface.
* Metadata to be specified in metadata.txt
* For tableX in query, there must exist a tableX.csv in the directory.

## Usage

```
	python 201564086.py <sql>
```

## Sample Examples

```
$ python 201564086.py "select * from table1;"
$ python 201564086.py "select max(A), max(B), min(A) from table3;"
$ python 201564086.py "select * from table3,table4 where table3.B = table4.B OR table3.B > 200;"
$ python 201564086.py "select * from table3,table4 where table3.B = table4.B AND table3.B > 200;"
$ python 201564086.py "select max(table3.B) from table3,table4 where table3.B = table4.B OR table3.B > 200;"
```