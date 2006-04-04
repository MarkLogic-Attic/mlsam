import module namespace sql  = "http://xqdev.com/sql" at "sql.xqy"
default function namespace = "http://www.w3.org/2003/05/xpath-functions"

sql:execute("describe zipcodes",
 "http://localhost:8080/mlsql", ())
