/*
 * Mark Logic Interface to Relational Databases
 *
 * Copyright 2006 Jason Hunter and Ryan Grimm
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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.net.*;
import javax.servlet.http.*;
import javax.servlet.*;
import org.jdom.input.SAXBuilder;
import org.jdom.*;
import org.jdom.output.XMLOutputter;

/**
 * Main class for supporting the sql.xqy client.
 *
 * Improvement idea: Let multiple executes happen in the same transaction.
 *
 * Improvement idea: Construct response XML document using strings rather
 * than JDOM for a performance improvement, but at the cost of sanity checking
 * and simplicity.
 */
public class MLSQL extends HttpServlet {

  private ConnectionPool pool = null;

  static String TRY_DATABASE_CONNECTION = "select 1";

  String initProblemDriverUnavailable = null;
  String initProblemMissingCredential = null;

  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    String driver = null, url, user, password;
    try {
      driver = getInitParameter("driver");
      url = getInitParameter("url");
      user = getInitParameter("user");
      password = getInitParameter("password");
      if (driver == null) {
        initProblemMissingCredential = "Error: web.xml file is missing the 'driver' init parameter";
        Log.log(initProblemMissingCredential);
      }
      if (url == null) {
        initProblemMissingCredential = "Error: web.xml file is missing the 'url' init parameter";
        Log.log(initProblemMissingCredential);
      }
      if (user == null) {
        Log.log("Warning: web.xml file is missing the 'user' init parameter");
      }
      if (password == null) {
        Log.log("Warning: web.xml file is missing the 'password' init parameter");
      }
      if (initProblemMissingCredential == null) {  // good to try
        pool = new ConnectionPool(driver, url, user, password);
      }
    }
    catch (ClassNotFoundException e) {  // db driver couldn't be found
      initProblemDriverUnavailable = "Could not load driver class '" + driver + "', unable to contact database";
      Log.log(initProblemDriverUnavailable);
    }
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    doPost(req, res);
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("text/xml");

    Namespace sql = Namespace.getNamespace("sql", "http://xqdev.com/sql");
    Document responseDoc = new Document();
    Element root = new Element("result", sql);
    Element meta = new Element("meta", sql);
    responseDoc.setRootElement(root);
    root.addContent(meta);

    Document requestDoc = null;
    try {
      // Normally the request comes via the post body,
      // but we let you bookmark w/ a query string
      String postbody = req.getParameter("postbody");
      if (postbody != null) {
        SAXBuilder builder = new SAXBuilder();
        requestDoc = builder.build(new StringReader(postbody));
//Log.log("Incoming: " + new XMLOutputter().outputString(requestDoc));
      }
      else {
        InputStream in = req.getInputStream();
        SAXBuilder builder = new SAXBuilder();
        requestDoc = builder.build(in);
//Log.log("Incoming: " + new XMLOutputter().outputString(requestDoc));
      }
    }
    catch (Exception e) {
      addExceptions(meta, e);
      // Now write the error and return
      OutputStream out = res.getOutputStream();
      new XMLOutputter().output(responseDoc, out);
      out.flush();
//Log.log("Returned: " + new XMLOutputter().outputString(responseDoc));
      return;
    }

    Connection con = null;
    try {
      Namespace[] namespaces = new Namespace[]{ sql };
      XPathHelper xpath = new XPathHelper(requestDoc, namespaces);

      String type = xpath.getString("/sql:request/sql:type");
      String query = xpath.getString("/sql:request/sql:query");
      int maxRows = xpath.getInt("/sql:request/sql:execute-options/sql:max-rows", -1);
      int queryTimeout = xpath.getInt("/sql:request/sql:execute-options/sql:query-timeout", -1);
      int maxFieldSize = xpath.getInt("/sql:request/sql:execute-options/sql:max-field-size", -1);
      List<Element> params = xpath.getElements("/sql:request/sql:execute-options/sql:parameters/sql:parameter");

      con = pool.getConnection();

      PreparedStatement stmt = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
      configureStatement(stmt, maxRows, queryTimeout, maxFieldSize);
//Log.log("params: " + params);
      parameterizeStatement(stmt, params);

      if (type.equalsIgnoreCase("select")) {
        try {
          ResultSet rs = stmt.executeQuery();
          addWarnings(meta, stmt.getWarnings());
          addResultSet(root, rs);
        }
        catch (SQLException e) {
          addExceptions(meta, e);
        }
      }
      else if (type.equalsIgnoreCase("update")) {
        try {
          int count = stmt.executeUpdate();
          addWarnings(meta, stmt.getWarnings());
          addUpdateCount(meta, count);
          stmt.getGeneratedKeys();
          addGeneratedKeys(meta, stmt.getGeneratedKeys());
        }
        catch (SQLException e) {
          addExceptions(meta, e);
        }
      }
      else {
        try {
          boolean isResultSet = stmt.execute();
          addWarnings(meta, stmt.getWarnings());
          if (isResultSet) {
            addResultSet(root, stmt.getResultSet());
          }
          else {
            addUpdateCount(meta, stmt.getUpdateCount());
            addGeneratedKeys(meta, stmt.getGeneratedKeys());
          }
        }
        catch (SQLException e) {
          addExceptions(meta, e);
        }
      }
    }
    catch (Exception e) {
      addExceptions(meta, e);
    }
    finally {
      if (con != null) pool.returnConnection(con);
    }

    OutputStream out = res.getOutputStream();
    new XMLOutputter().output(responseDoc, out);
    out.flush();
//Log.log("Returned: " + new XMLOutputter().outputString(responseDoc));
  }

  private static void addUpdateCount(Element meta, int count) {
    Namespace sql = meta.getNamespace();
    meta.addContent(
            new Element("rows-affected", sql).setText("" + count)
    );
  }

  private static void addGeneratedKeys(Element meta, ResultSet keys) throws SQLException {
    Namespace sql = meta.getNamespace();
    while (keys.next()) {  // should only be one
      meta.addContent(
              new Element("generated-key", sql).setText("" + keys.getString(1))
      );
    }
  }

  private static void addResultSet(Element root, ResultSet rs) throws SQLException {
    Namespace sql = root.getNamespace();

    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    while (rs.next()) {
      Element tuple = new Element("tuple", sql);
      for (int i = 1; i <= columnCount; i++) {
        String colName = rsmd.getColumnName(i);  // names aren't guaranteed OK in xml
        int colType = rsmd.getColumnType(i);
        String colTypeName = rsmd.getColumnTypeName(i);
        String colValue = rs.getString(i);
        boolean wasNull = rs.wasNull();
        Element elt = new Element(colName);
        if (wasNull) {
          elt.setAttribute("null", "true");
        }
        if ("UNKNOWN".equalsIgnoreCase(colTypeName)) {
          tuple.addContent(elt.setText("UNKNOWN TYPE"));  // XXX ugly
        }
        else {
          tuple.addContent(elt.setText(colValue));
        }
      }
      root.addContent(tuple);
    }
  }

  /*
  private static boolean isKnownType(int type) {
    return (type == Types.BIGINT ||
            type == Types.BOOLEAN ||
            type == Types.DATE ||
            type == Types.DECIMAL ||
            type == Types.DOUBLE ||
            type == Types.FLOAT ||
            type == Types.INTEGER ||
            type == Types.LONGVARCHAR ||
            type == Types.NULL ||
            type == Types.NUMERIC ||
            type == Types.SMALLINT ||
            type == Types.TIME ||
            type == Types.TIMESTAMP ||
            type == Types.TINYINT ||
            type == Types.VARCHAR);
  }
  */

  private static void addExceptions(Element meta, Throwable t) {
    if (t == null) return;

    Namespace sql = meta.getNamespace();
    Element exceptions = new Element("exceptions", sql);
    meta.addContent(exceptions);
    do {
      exceptions.addContent(
              new Element("exception", sql)
                      .setAttribute("type", t.getClass().getName())
                      .addContent(new Element("reason", sql).setText(t.getMessage()))
      );
      Log.log(t);
      t = t.getCause();
    } while (t != null);
  }

  private static void addExceptions(Element meta, SQLException e) {
    if (e == null) return;

    Namespace sql = meta.getNamespace();
    Element exceptions = new Element("exceptions", sql);
    meta.addContent(exceptions);
    do {
      exceptions.addContent(
              new Element("exception", sql)
                      .setAttribute("type", e.getClass().getName())
                      .addContent(new Element("reason", sql).setText(e.getMessage()))
                      .addContent(new Element("sql-state", sql).setText(e.getSQLState()))
                      .addContent(new Element("vendor-code", sql).setText("" + e.getErrorCode()))
      );
//Log.log(e);
      e = e.getNextException();
    } while (e != null);
  }

  private static void addWarnings(Element meta, SQLWarning w) {
    if (w == null) return;

    Namespace sql = meta.getNamespace();
    Element warnings = new Element("warnings", sql);
    meta.addContent(warnings);
    do {
      warnings.addContent(
              new Element("warning", sql)
                      .setAttribute("type", w.getClass().getName())
                      .addContent(new Element("reason", sql).setText(w.getMessage()))
                      .addContent(new Element("sql-state", sql).setText(w.getSQLState()))
                      .addContent(new Element("vendor-code", sql).setText("" + w.getErrorCode()))
      );
//Log.log(w);
      w = w.getNextWarning();
    } while (w != null);
  }

  private static void configureStatement(PreparedStatement stmt, int maxRows, int queryTimeout, int maxFieldSize)
        throws SQLException {
    if (maxRows != -1) {
      stmt.setMaxRows(maxRows);
    }
    if (queryTimeout != -1) {
      stmt.setQueryTimeout(queryTimeout);
    }
    if (maxFieldSize != -1) {
      stmt.setMaxFieldSize(maxFieldSize);
    }
  }

  private static void parameterizeStatement(PreparedStatement stmt, List<Element> params)
          throws SQLException, MalformedURLException, NumberFormatException {
    // Presently we accept these types:
    // boolean, date, double, float, int,
    // long, short, string, time, timestamp.
    // We also accept a null flag.
    // XXX Might be nice to support blobs and clobs, BigDecimal
    int paramPosition = 0;
    for (Element param : params) {
      paramPosition++;
      String paramType = param.getAttributeValue("type");
      String paramValue = param.getText();
      boolean paramNull = "true".equalsIgnoreCase(param.getAttributeValue("null"));

      if (paramType == null) {
        String s = "Null parameter type received: " + paramType + " with value: " + paramValue;
        Log.log(s);
        throw new RuntimeException(s);
      }
      else if (paramType.equalsIgnoreCase("boolean")) {
        if (paramNull) { stmt.setNull(paramPosition, Types.BOOLEAN); }  // MySQL seems to ignore types
        else { stmt.setBoolean(paramPosition, new Boolean(paramValue).booleanValue()); }
      }
      else if (paramType.equalsIgnoreCase("date")) {  // dates come as long values
        if (paramNull) { stmt.setNull(paramPosition, Types.DATE); }
        else { stmt.setDate(paramPosition, new java.sql.Date(new Long(paramValue))); }
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
        else { stmt.setTime(paramPosition, new java.sql.Time(new Long(paramValue))); }
      }
      else if (paramType.equalsIgnoreCase("timestamp")) {
        if (paramNull) { stmt.setNull(paramPosition, Types.TIMESTAMP); }
        else { stmt.setTimestamp(paramPosition, new java.sql.Timestamp(new Long(paramValue))); }
      }
      else {
        String s = "Unknown parameter type received: " + paramType + " with value: " + paramValue;
        Log.log(s);
        throw new RuntimeException(s);
      }
    }
  }
}
