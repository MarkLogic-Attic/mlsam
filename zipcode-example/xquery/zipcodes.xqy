module "http://xqdev.com/zipcodes"
declare namespace zip = "http://xqdev.com/zipcodes"
import module namespace sql = "http://xqdev.com/sql" at "sql.xqy"
default function namespace = "http://www.w3.org/2003/05/xpath-functions"

define function zip:within(
    $zip as xs:string,
    $distance as xs:double
) as element(area)*
{
    let $host := "http://69.107.73.11:8080/mlsql"
    let $res := sql:execute("select latitude, longitude from zipcodes where zipcode = ?", $host, sql:params($zip))
    let $givenLat := xs:decimal($res//latitude)
    let $givenLong := xs:decimal($res//longitude)
    let $distancePlus := $distance + $distance * .15
    let $latamt := $distancePlus div 69.1
    let $longamt := ($distancePlus div math:cos($givenLat div 57.3)) div 69.1
    let $minx := string($givenLat - $latamt)
    let $maxx := string($givenLat + $latamt)
    let $miny := string($givenLong - $longamt)
    let $maxy := string($givenLong + $longamt)
    let $poly := concat("Polygon((", $minx, " ", $miny, ", ", $maxx, " ", $miny, ", ", $maxx, " ", $maxy, ", ", $minx, " ", $maxy, ", ", $minx, " ", $miny, "))")
    let $query := concat("
        select zipcode, city, state, round( 3963.0 *
                acos( sin(", string($givenLat), " / 57.2958) * sin( latitude / 57.2958) +
                cos(", string($givenLat), " / 57.2958) * cos(latitude / 57.2958) *  cos(longitude / 57.2958 - ", string($givenLong), " / 57.2958))
            , 2) as distance
        from zipcodes
        where within(geo, GeomFromText('", $poly, "'))
            and 
            ( 3963.0 *
                acos( sin(", string($givenLat), " / 57.2958) * sin( latitude / 57.2958) +
                cos(", string($givenLat), " / 57.2958) * cos(latitude / 57.2958) *  cos(longitude / 57.2958 - ", string($givenLong), " / 57.2958))
            ) <= ?
        order by distance")
    let $zips := sql:execute($query, $host, sql:params($distance))
    for $i in $zips/sql:tuple
    return <area>{ $i/* }</area>
}
