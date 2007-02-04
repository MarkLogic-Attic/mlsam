/*
 * Mark Logic Interface to Relational Databases
 *
 * Copyright 2007 Jason Hunter, Ryan Grimm, and Will LaForest
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Jason Hunter
 * @version 1.0
 *
 */

package com.xqdev.sql;

import java.sql.Types;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Handles mappings between parameter types passed in the <code>type</code> attribute and
 * SQL type oriented methods (like <code>registerOutParameter</code> and <code>getObject</code>).
 */
class TypeMapping {

  private static final String ISO_DATE_PATTERN = "yyyy-MM-ddZ";
  private static final DateFormat DATE_PARSER = new SimpleDateFormat(ISO_DATE_PATTERN);
  private static final String ISO_TIME_PATTERN = "HH:mm:ss.SSSZ";
  private static final DateFormat TIME_PARSER = new SimpleDateFormat(ISO_TIME_PATTERN);
  private static final String ISO_DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final DateFormat DATETIME_PARSER = new SimpleDateFormat(ISO_DATETIME_PATTERN);

  public static int getSqlDataType(String type) {
    if (type.equalsIgnoreCase("boolean"))
      return Types.BOOLEAN;
    else if (type.equalsIgnoreCase("date"))
      return Types.DATE;
    else if (type.equalsIgnoreCase("double"))
      return Types.DOUBLE;
    else if (type.equalsIgnoreCase("float"))
      return Types.FLOAT;
    else if (type.equalsIgnoreCase("int"))
      return Types.INTEGER;
    else if (type.equalsIgnoreCase("long"))
      return Types.BIGINT;
    else if (type.equalsIgnoreCase("short"))
      return Types.SMALLINT;
    else if (type.equalsIgnoreCase("string"))
      return Types.VARCHAR;
    else if (type.equalsIgnoreCase("time"))
      return Types.TIME;
    else if (type.equalsIgnoreCase("timestamp"))
      return Types.TIMESTAMP;
    else if (type.equalsIgnoreCase("blob"))
      return Types.BLOB;
    else if (type.equalsIgnoreCase("longvarbinary"))
      return Types.LONGVARBINARY;
    else {
      String s = "Unknown parameter type received: " + type + ".";
      Log.log(s);
      throw new RuntimeException(s);
    }
  }

  /**
   * Given the <code>CallableStatement</code> and the param type attribute value call the
   * correct getXXX on the <code>CallableStatement</code> and return a stringified version.
   *
   * @param callableStmt
   * @param type
   * @param index
   * @throws SQLException
   * @return a stringified result.
   */
  public static String getStringValue(CallableStatement callableStmt, String type, int index)
                              throws SQLException {

    Object returnObject = null;

    if (type.equalsIgnoreCase("boolean"))
      returnObject = callableStmt.getBoolean(index);
    else if (type.equalsIgnoreCase("date"))
      returnObject = callableStmt.getDate(index);
    else if (type.equalsIgnoreCase("double"))
      returnObject = callableStmt.getDouble(index);
    else if (type.equalsIgnoreCase("float"))
      returnObject = callableStmt.getFloat(index);
    else if (type.equalsIgnoreCase("int"))
      returnObject = callableStmt.getInt(index);
    else if (type.equalsIgnoreCase("long"))
      returnObject = callableStmt.getLong(index);
    else if (type.equalsIgnoreCase("short"))
      returnObject = callableStmt.getShort(index);
    else if (type.equalsIgnoreCase("string"))
      returnObject = callableStmt.getString(index);
    else if (type.equalsIgnoreCase("time"))
      returnObject = callableStmt.getTime(index);
    else if (type.equalsIgnoreCase("timestamp"))
      returnObject = callableStmt.getTimestamp(index);
    else if (type.equalsIgnoreCase("blob"))
      returnObject = callableStmt.getBlob(index);
    else if (type.equalsIgnoreCase("longvarbinary"))
      returnObject = callableStmt.getObject(index);
    else {
      String s = "Unknown parameter type received: " + type;
      Log.log(s);
      throw new RuntimeException(s);
    }

    return returnObject.toString();
  }

  /**
   * Given the param type attribute value interpret paramValue correctly and call the
   * correct setter method on the <code>PreparedStatement</code>.
   *
   * @param paramType param type attribute value
   * @param paramNull whether the parameter was specified to be null
   * @param stmt <code>PreparedStatement</code> to set values on.
   * @param paramPosition the parameter position to use with the set call.
   * @param paramValue value for the parameter.
   * @throws SQLException
   * @throws ParseException
   */
  public static void parameterize(String paramType, boolean paramNull, PreparedStatement stmt,
           int paramPosition, String paramValue) throws SQLException, ParseException {

    if (paramType.equalsIgnoreCase("boolean")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.BOOLEAN); }  // MySQL seems to ignore types
      else { stmt.setBoolean(paramPosition, new Boolean(paramValue).booleanValue()); }
    }
    else if (paramType.equalsIgnoreCase("date")) {  // dates come as long values
      if (paramNull) { stmt.setNull(paramPosition, Types.DATE); }
      else {
        int lastIndex = paramValue.lastIndexOf(':');
        String fixedFormat = paramValue.substring(0, lastIndex) +
                paramValue.substring(lastIndex + 1, paramValue.length());

        Date date = DATE_PARSER.parse(fixedFormat);
        stmt.setDate(paramPosition, new java.sql.Date(date.getTime()));
      }
    }
    else if (paramType.equalsIgnoreCase("double")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.DOUBLE); }
      else { stmt.setDouble(paramPosition, new Double(paramValue)); }
    }
    else if (paramType.equalsIgnoreCase("float")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.FLOAT); }
      else { stmt.setFloat(paramPosition, new Float(paramValue)); }
    }
    else if (paramType.equalsIgnoreCase("int")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.INTEGER); }
      else { stmt.setInt(paramPosition, new Integer(paramValue)); }
    }
    else if (paramType.equalsIgnoreCase("long")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.BIGINT); }
      else { stmt.setLong(paramPosition, new Long(paramValue)); }
    }
    else if (paramType.equalsIgnoreCase("short")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.SMALLINT); }
      else { stmt.setShort(paramPosition, new Short(paramValue)); }
    }
    else if (paramType.equalsIgnoreCase("string")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.VARCHAR); }
      else { stmt.setString(paramPosition, paramValue); }
    }
    else if (paramType.equalsIgnoreCase("time")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.TIME); }
      else {
        int lastIndex = paramValue.lastIndexOf(':');
        String fixedFormat = paramValue.substring(0, lastIndex) +
                paramValue.substring(lastIndex + 1, paramValue.length());
        Date date = TIME_PARSER.parse(fixedFormat);
        stmt.setTime(paramPosition, new Time(date.getTime()));
      }
    }
    else if (paramType.equalsIgnoreCase("timestamp")) {
      if (paramNull) { stmt.setNull(paramPosition, Types.TIMESTAMP); }
      else {
        int lastIndex = paramValue.lastIndexOf(':');
        String fixedFormat = paramValue.substring(0, lastIndex) +
                paramValue.substring(lastIndex + 1, paramValue.length());
        Date date = DATETIME_PARSER.parse(fixedFormat);
        stmt.setTimestamp(paramPosition, new Timestamp(date.getTime()));
      }
    }
   // blob is not supported but we do want to allow for null blob parameters
    else if (paramType.equalsIgnoreCase("blob")) {
      stmt.setNull(paramPosition, Types.BLOB);
    }
    // longvarbinary is not supported but we do want to allow for null longvarbinary parameters
    else if (paramType.equalsIgnoreCase("longvarbinary")) {
      stmt.setNull(paramPosition, Types.LONGVARBINARY);
    }
    else {
      String s = "Unknown parameter type received: " + paramType + " with value: " + paramValue;
      Log.log(s);
      throw new RuntimeException(s);
    }
  }
}