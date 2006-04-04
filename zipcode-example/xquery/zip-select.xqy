import module namespace sql  = "http://xqdev.com/sql" at "sql.xqy"
default function namespace = "http://www.w3.org/2003/05/xpath-functions"

sql:execute("select zipcode, city, state, AsText(geo) as geo from zipcodes
where zipcode = ?", "http://localhost:8080/mlsql", sql:params(95070))
