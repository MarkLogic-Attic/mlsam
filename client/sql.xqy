(:~
 : Mark Logic Interface to Relational Databases
 :
 : For a tutorial please see
 : http://xqzone.marklogic.com/howto/tutorials/2006-04-mlsql.xqy.
 :
 : Copyright 2006 Jason Hunter and Ryan Grimm
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 :     http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
 :
 : @author Jason Hunter and Ryan Grimm
 : @version 1.0.1
 :)

module "http://xqdev.com/sql"
declare namespace sql = "http://xqdev.com/sql"
default function namespace = "http://www.w3.org/2003/05/xpath-functions"


(:~
 : Executes the SQL statement specified in $query.
 :
 : (For a tutorial please see
 : http://xqzone.marklogic.com/howto/tutorials/2006-04-mlsql.xqy)
 :
 : @param $query The SQL query to be executed
 :
 : @param $url The url to send queries to
 :
 : @param $options Query options.  These include:
 :        &lt;sql:max-rows&gt; - Max number of rows to return
 :        &lt;sql:query-timeout&gt; - Timeout for the query
 :        &lt;sql:max-field-size&gt; - Max size of any field
 :        &lt;sql:parameters&gt; - Bind parameters, order matters
 :            &lt;sql:parameter type="int"&gt;10&lt;/sql:parameter&gt;
 :            ...
 :        &lt;/sql:parameters&gt;
 :
 : @return An xml document with the result of the query
 :
 :)
define function sql:execute(
  $query as xs:string,
  $uri as xs:string,
  $options as element(sql:execute-options)?
) as element() 
{
  sql:_call($query, $uri, "execute", $options)
}


(:~
 : Executes the SQL statement specified in $query.  Must be a select 
 : query.
 :
 : @param $query The SQL select query to be executed
 :
 : @param $url The url to send queries to
 :
 : @param $options Query options.  These include:
 :        &lt;sql:max-rows&gt; - Max number of rows to return
 :        &lt;sql:query-timeout&gt; - Timeout for the query
 :        &lt;sql:max-field-size&gt; - Max size of any field
 :        &lt;sql:parameters&gt; - Bind parameters, order matters
 :            &lt;sql:parameter type="int"&gt;10&lt;/sql:parameter&gt;
 :            ...
 :        &lt;/sql:parameters&gt;
 :
 : @return An xml document with the result of the query
 :
 :)
define function sql:executeQuery(
  $query as xs:string,
  $uri as xs:string,
  $options as element(sql:execute-options)?
) as element(sql:result)
{
  sql:_call($query, $uri, "select", $options)
}


(:~
 : Executes the SQL statement specified in $query.  Must be an update 
 : query.
 :
 : @param $query The SQL update query to be executed
 :
 : @param $url The url to send queries to
 :
 : @param $options Query options.  These include:
 :        &lt;sql:max-rows&gt; - Max number of rows to return
 :        &lt;sql:query-timeout&gt; - Timeout for the query
 :        &lt;sql:max-field-size&gt; - Max size of any field
 :        &lt;sql:parameters&gt; - Bind parameters, order matters
 :            &lt;sql:parameter type="int"&gt;10&lt;/sql:parameter&gt;
 :            ...
 :        &lt;/sql:parameters&gt;
 :
 : @return An xml document with the result of the query
 :
 :)
define function sql:executeUpdate(
  $query as xs:string,
  $uri as xs:string,
  $options as element(sql:execute-options)?
) as element(sql:result)
{
  sql:_call($query, $uri, "update", $options)
}


(:~
 : Convenience function that introspects the sequence of atomics passed
 : to it and returns a &lt;sql:execute-options&gt; node which can then
 : be passed to sql:execute().
 :
 : @param $params Sequence of atomics to use as bind parameters
 :
 : @return An &lt;sql:execute-options&gt; xml structure
 :
 :)
define function sql:params($params as item()*)
                                 as element(sql:execute-options) {
  sql:opts($params, (), (), ())
}

(:~
 : Convenience function that introspects the sequence of atomics passed
 : to it as well as the max rows, query timeout, and max field size options,
 : and returns a &lt;sql:execute-options&gt; node which can then
 : be passed to sql:execute().  Any parameter not specified allows the
 : default values to be used.
 :
 : @param $params Sequence of atomics to use as bind parameters
 :
 : @param $maxRows Maximum number of rows to return
 :
 : @param $queryTimeout Sets the number of seconds the driver should wait
 :   for a statement to execute
 :
 : @param $maxFieldSize Sets the max number of bytes that can be returned
 :   for character and binary column values (only applies to BINARY,
 :   VARBINARY, LONGVARBINARY, CHAR, VARCHAR, and LONGVARCHAR columns).
 :   Any data exceeding the limit gets silently discarded.
 :
 : @return An &lt;sql:execute-options&gt; xml structure
 :
 :)
define function sql:opts($params as item()*,
                         $maxRows as xs:integer?,
                         $queryTimeout as xs:integer?,
                         $maxFieldSize as xs:integer?)
                                 as element(sql:execute-options) {
  <sql:execute-options>
    { sql:_getParameters($params) }
    { if (exists($maxRows)) then
          <sql:max-rows>{$maxRows}</sql:max-rows> else () }
    { if (exists($queryTimeout)) then
          <sql:query-timeout>{$queryTimeout}</sql:query-timeout> else () }
    { if (exists($maxFieldSize)) then
          <sql:max-field-size>{$maxFieldSize}</sql:max-field-size> else () }
  </sql:execute-options>
}




(:
 : Support calls.
 : The leading underscore indicates to consider these private.
 :)

define function sql:_call(
  $query as xs:string,
  $uri as xs:string,
  $mode as xs:string,
  $options as element(sql:execute-options)?
) as element()
{
  let $data :=
    <sql:request xmlns:sql="http://xqdev.com/sql">
      <sql:type>{ $mode }</sql:type>
      <sql:query>{ $query }</sql:query>
      { $options }
    </sql:request>
  let $exceptions := sql:_checkBindParams($options)
  return
    if(count($exceptions))
    then sql:_outputExceptions($exceptions, $mode)
    else try {
        let $response :=
          xdmp:http-post($uri, <options xmlns="xdmp:http">
              <data>{ xdmp:quote($data) }</data>
            </options>
          )
        let $code := xs:integer($response[1]/*:code)
        let $exceptions := if ($code != 200) then <sql:exception><sql:reason>Invalid http response code: { $code } { $response[1]/text() }</sql:reason></sql:exception> else ()
        return if(count($exceptions))
          then sql:_outputExceptions($exceptions, $mode) else $response[2]/*
      }
      catch($e) {
        sql:_outputExceptions(<sql:exception><sql:reason>{ string($e/*:code) }: { string-join($e/*:data/*:datum, " ") }</sql:reason></sql:exception>, $mode)
      }
}

define function sql:_checkBindParams(
  $options as element(sql:execute-options)?
) as element(sql:exception)*
{
  for $i at $count in $options/sql:parameters/sql:parameter
  return
    (
    if(empty($i/@type)) then <sql:exception><sql:reason>Bind parameter: { $count } is missing a type</sql:reason></sql:exception> else ()
    ,
    if(not($i/@type = (
          "boolean", "date", "double", "float", "int",
          "long", "short", "string", "time", "timestamp"
        ))) then <sql:exception><sql:reason>Bind parameter: { $count } has an invalid type: '{ $i/@type }'</sql:reason></sql:exception> else ()
    )
}

define function sql:_outputExceptions(
  $exceptions as element(sql:exception)+,
  $mode as xs:string
) as element(sql:result)
{
  <sql:result xmlns:sql="http://xqdev.com/sql">
    <sql:meta>
      <sql:exceptions>{ $exceptions }</sql:exceptions>
    </sql:meta>
  </sql:result>
}


(: I'm not 100% sure about each of these mappings :)
define function sql:_getType($item as item()) {
  typeswitch ($item)
    case xs:boolean return "boolean"
    case xs:double return "double"
    case xs:float return "float"
    case xs:short return "short"
    case xs:long return "long"
    case xs:decimal return "int"
    case xs:string return "string"
    case xs:date return "date"
    case xs:time return "time"
    case xs:dateTime return "timestamp"
    default return "string"
}

define function sql:_getParameters($items as item()*)
                      as element(sql:parameters) {
  <sql:parameters>
  {
    for $item in $items
    return
    <sql:parameter>
      { attribute type { sql:_getType($item) } }
      { string($item) }
    </sql:parameter>
  }
  </sql:parameters>
}

