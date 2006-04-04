Database vendors don't permit third parties to distribute their JDBC JARs, so
you'll need to locate these on your own.  To help, here are some sample JAR
file names (some JDBC drivers require multiple JARs):

- Oracle: ojdbc14.jar
- IBM DB2: db2jcc.jar and db2jcc_license_cu.jar
- Microsoft SQL Server: msbase.jar, mssqlserver.jar, and msutil.jar
- MySQL: mysql-connector-3.0.16.jar
- Derby: derby.jar and derbytools.jar (when embedded)
