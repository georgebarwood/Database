# Database
SQL database implemented in C#

Guide to implementation source files:

Public.cs = public interface.

WebServer.cs = main program, http web server ( example client ).

Init.cs = SQL initialisation script.

SQL-independent ( namespace DBNS )
================================

Database.cs = implements Database.

Log.cs = log file to ensure atomic updates.

Stream.cs = fully buffered stream for Rollback/Commit.

Table.cs = implementation of TABLE.

IndexFile.cs, IndexPage.cs = implementation of INDEX.

SQL-specific ( namespace SQLNS )
================================

SqlExec.cs = parsing and execution of SQL statements.

Block.cs = list of statements for execution.

Exp.cs, ExpStd.Exp = scalar expressions.

TableExp.cs = table-valued expressions.

Group.cs = implementation of GROUP BY.

Sort.cs = implementation of ORDER BY.

IdSet.cs = optimisation of WHERE.

Util.cs = various utility classes.

