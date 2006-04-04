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

/**
 * Utility class used for logging messages, typically errors and warnings.
 * It can output to System.err and/or a log file.  It may also timestamp
 * the message.
 */
public class Log {

  public static void log(String msg) {
    System.err.println(getPrefix() + msg);
  }

  public static void log(Throwable e) {
    System.err.println(getPrefix());
    e.printStackTrace(System.err);
  }

  public static void log(String msg, Throwable e) {
    System.err.println(getPrefix() + msg);
    e.printStackTrace(System.err);
  }

  protected static String getPrefix() {
    return "MarkLogic SQL Connector: ";
    /*
    // This is useful for writing to a file
    StringBuffer buf = new StringBuffer();
    buf.append("[");
    buf.append(new Date().toString());
    buf.append("] ");
    return buf.toString();
    */
  }
}
