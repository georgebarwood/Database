# Database
SQL database implemented in C#

Guide to implementation source files:

Public.cs = public interface.

WebServer.cs = main program, http web server ( example client ).

Init.cs = SQL initialisation script.

Compile.bat is file to compile source ( will need editing depending on version of .NET you have installed ).

SQL-independent ( namespace DBNS )
================================

Database.cs = implements Database.

Log.cs = log file to ensure atomic updates.

Stream.cs = fully buffered stream for Rollback/Commit.

Table.cs = implementation of TABLE.

IndexFile.cs, IndexPage.cs = implementation of INDEX.

Util.cs = various utility classes.

SQL-specific ( namespace SQLNS )
================================

SqlExec.cs = parsing and execution of SQL statements.

Block.cs = list of statements for execution.

Exp.cs, ExpStd.cs, ExpConv.cs = scalar expressions.

TableExp.cs = table-valued expressions.

Group.cs = implementation of GROUP BY.

Sort.cs = implementation of ORDER BY.

IdSet.cs = optimisation of WHERE.

Configuration
=============

The database files are stored in C:\Databasefiles\Test\ this directory needs to be created.
See webserver.cs to change the location. Also, permission to listen needs to be granted, e.g.

netsh http add urlacl url=http://+:8080/ user=GEORGE-DELL\pc

Again, see webserver.cs to change the http setup. If localhost is used, no permission is needed.

Implementation details
======================

Database tables are kept in files with a fixed record size. This is possible because variable length values ( string and binary ) are encoded and stored seperately. The encoding is currently simply the offset in the system string or binary file.

The other type of file is an index file. Index files are B-Trees where each page has a fixed maximum size ( 4 kb at the time of writing ), when a page overflows it is split, and the new page is recorded in a parent page. See indexpage.cs for the details.

Manual
======
When the webserver runs, a link to a manual included with the program is displayed. Alternatively see https://georgebarwood.github.io/ for a copy of the manual.

