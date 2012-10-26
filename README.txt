MLSAM is an open source library that allows easy access to relational database
systems from within the MarkLogic environment.

If you are looking for SQL access to a MarkLogic database,
please see http://docs.marklogic.com/guide/sql

The MLSAM Tutorial has more information:
http://developer.marklogic.com/learn/2006-04-mlsam.xqy

(MLSAM was originally named MLSQL).  

Source Components
-----------------

The "client" directory holds the sql.xqy library module.  That's the only file
needed from XQuery.

The "server" directory holds the servlet with which the XQuery communicates.
The Java code is straightforward: it accepts an XML request from the XQuery
client, issues a JDBC request to the relational database, and returns an XML
response to the XQuery client.

The "zipcode-example" directory holds the SQL zipcode table data and example
queries as described in the tutorial.


Building
--------

Just run "ant".  It produces zip files under the "deliverable" directory and
a "buildtmp" support directory.


Installation
------------

The tutorial above explains the basic steps of an install.

