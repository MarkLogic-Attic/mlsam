(:~
 : Mark Logic Interface to Relational Databases
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
 : @author
 : @version 1.0
 :
 :)

module "http://xqdev.com/sql"
declare namespace sql = "http://xqdev.com/sql"
default function namespace = "http://www.w3.org/2003/05/xpath-functions"


(:~
 : Executes any SQL statement specified in $query
 :
 : @param $query The SQL query to be executed
 :
 : @param $url The url to send queries to
 :
 : @param $options Query options.  These include:
 :        <sql:max-rows> - Max number of rows to return
 :        <sql:query-timeout> - Timeout for the query
 :        <sql:max-field-size> - Max size of any field
 :        <sql:parameters> - Bind parameters, order matters
 :            <sql:parameter>10</sql:parameter>
 :            ...
 :        </sql:parameters>
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


define function sql:executeQuery(
  $query as xs:string,
  $uri as xs:string,
  $options as element(sql:execute-options)?
) as element(sql:result)
{
  sql:_call($query, $uri, "select", $options)
}


define function sql:executeUpdate(
  $query as xs:string,
  $uri as xs:string,
  $options as element(sql:execute-options)?
) as element(sql:result)
{
  sql:_call($query, $uri, "update", $options)
}


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
          "bigdecimal", "boolean", "date", "double", "float", "int",
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


(: A few support calls :)

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

define function sql:params($params as item()*)
                                 as element(sql:execute-options) {
  sql:opts($params, (), (), ())
}

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
