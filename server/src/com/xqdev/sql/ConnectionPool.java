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

import java.sql.*;
import java.util.*;

/**
 * Basic database connection pool class.
 */
public class ConnectionPool {
  private Hashtable connections = new Hashtable();
  private Properties props;

  public ConnectionPool(String driver, String url,
                        String user, String password)
                   throws ClassNotFoundException {
    props = new Properties();
    props.put("driver", driver);
    props.put("url", url);
    props.put("user", user);
    props.put("password", password);
    initializePool(props);
  }

  public Connection getConnection() throws SQLException {
    Connection con = null;

    Enumeration cons = connections.keys();

    synchronized (connections) {
      while(cons.hasMoreElements()) {
        con = (Connection)cons.nextElement();

        Boolean b = (Boolean)connections.get(con);
        if (b == Boolean.FALSE) {
          // So we found an unused connection.                       
          // Test its integrity with a quick setAutoCommit(true) call.
          // For production use, more testing should be performed,   
          // such as executing a simple query.                       
          Statement stmt = null;
          try {
            stmt = con.createStatement();
            stmt.execute(MLSQL.TRY_DATABASE_CONNECTION);
          }
          catch(SQLException e) {
            // Problem with the connection, replace it.              
            // First close the connection to be replaced to avoid leaks
            con.close();
            connections.remove(con);
            con = getNewConnection();
          }
          finally {
            if (stmt != null) {
              try { stmt.close(); } catch (SQLException ignored) { }
            }
          }
          // Update the Hashtable to show this one's taken
          connections.put(con, Boolean.TRUE);
          // Return the connection                                   
          return con;
        }
      }

      // If we get here, there were no free connections.  Make one more.
      // A more robust connection pool would have a maximum size limit,
      // and would reclaim connections after some timeout period
      con = getNewConnection();
      connections.put(con, Boolean.TRUE);
      return con;
    }
  }

  public void returnConnection(Connection returned) {
    if (connections.containsKey(returned)) {
      connections.put(returned, Boolean.FALSE);
    }
  }

  private void initializePool(Properties props)
                   throws ClassNotFoundException {
    // Load the driver
    Class.forName(props.getProperty("driver"));
  }

  private Connection getNewConnection() throws SQLException {
    return DriverManager.getConnection(
      props.getProperty("url"), props);
  }
}                                                                    
