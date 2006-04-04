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

import java.util.*;
import java.text.*;
import java.util.regex.*;
import org.jdom.*;
import org.jdom.output.*;
import org.jdom.xpath.XPath;

/**
 * Package protected class to simplify XPath work.
 */
class XPathHelper {

	Object context;
  Namespace[] namespaces;

  public XPathHelper(Object context) {
		this.context = context;
	}

  public XPathHelper(Object context, Namespace[] namespaces) {
    this.context = context;
    this.namespaces = namespaces;
  }

  public Object getNode(String xpath) throws XPathHelperException {
		try {
			XPath path = XPath.newInstance(xpath);
      for (Namespace n : namespaces) path.addNamespace(n);
      return path.selectSingleNode(context);
		}
		catch (JDOMException e) {
			throw new XPathHelperException(e);
		}
	}

	// Returns the Element matching the xpath as an XML string or "" if none
	public String getElementAsString(String xpath) throws XPathHelperException {
		try {
			XPath path = XPath.newInstance(xpath);
      for (Namespace n : namespaces) path.addNamespace(n);
      Object node = path.selectSingleNode(context);
			if (node instanceof Element) {
				Element element = (Element) node;
				XMLOutputter outputter = new XMLOutputter();
				return outputter.outputString(element);
			}
			if (node == null) {
				return null;
			}
			throw new XPathHelperException("XPath '" + xpath + "' failed to return as Element, returned: " + node.getClass().getName());
		}
		catch (JDOMException e) {
			throw new XPathHelperException(e);
		}
	}

	// Returns the string matching the xpath or "" if none
	public String getString(String xpath) throws XPathHelperException {
		try {
			XPath path = XPath.newInstance(xpath);
      for (Namespace n : namespaces) path.addNamespace(n);
      Object node = path.selectSingleNode(context);
			if (node instanceof Text) {
			  return ((Text) node).getTextTrim();
			}
			if (node instanceof Element) {
			  return ((Element) node).getTextTrim();
			}
			if (node instanceof Attribute) {
				return ((Attribute) node).getValue();
			}
			if (node == null) {
				return "";
			}
			throw new XPathHelperException("XPath '" + xpath + "' failed to return as string, returned: " + node.getClass().getName());
		}
		catch (JDOMException e) {
			throw new XPathHelperException(e);
		}
	}
	
	public String getString(String xpath, String defaultValue) {
		try {
			String value = getString(xpath);
			if ("".equals(value)) {
				return defaultValue;
			} else {
				return value;
			}
		}
		catch (Exception e) {
			return defaultValue;
		}
	}

	public long getLong(String xpath) throws XPathHelperException {
		String s = getString(xpath);
		try {
			return Long.parseLong(s);
		}
		catch (NumberFormatException e) {
			throw new XPathHelperException("XPath '" + xpath + "' failed to parse as long: " + e.getMessage(), e);
		}
	}

	public double getDouble(String xpath) throws XPathHelperException {
		String s = getString(xpath);
		try {
			return Double.parseDouble(s);
		}
		catch (NumberFormatException e) {
			throw new XPathHelperException("XPath '" + xpath + "' failed to parse as double: " + e.getMessage(), e);
		}
	}

	public int getInt(String xpath) throws XPathHelperException {
		String s = getString(xpath);
		try {
			return Integer.parseInt(s);
		}
		catch (NumberFormatException e) {
			throw new XPathHelperException("XPath '" + xpath + "' failed to parse as int: " + e.getMessage(), e);
		}
	}
	
	public int getInt(String xpath, int defaultValue) throws XPathHelperException {
		try {
			return (getInt(xpath));
		} catch(Exception e) {
			return defaultValue;
		}
	}
	
	public Integer getInteger(String xpath) throws XPathHelperException {
		return new Integer(getInt(xpath));
	}
	
	public Integer getInteger(String xpath, Integer defaultValue) {
		try {
			return (getInteger(xpath));
		}
		catch (Exception e) {
			return defaultValue;
		}
	}

	public boolean getBoolean(String xpath) throws XPathHelperException {
		String s = getString(xpath);
		return Boolean.valueOf(s).booleanValue();
		// No exceptions thrown here
	}

	public boolean getBoolean(String xpath, boolean defaultValue) {
		try {
			return getBoolean(xpath);
		}
		catch (Exception e) {
			return defaultValue;
		}
	}


	private Date parseDate(String date) throws ParseException {
		Pattern pattern = Pattern.compile("(.*):(\\d\\d)");
		Matcher matcher = pattern.matcher(date);
		date = matcher.replaceFirst("$1$2");

		// Try the long date (for project timestamps) with failures trying short
		// dates (for book or article printings).
		try {
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ").parse(date);
		}
		catch (ParseException e) {
			// keep trying below
		}

		try {
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(date);
		}
		catch(ParseException e) {
			// keep trying below
		}

		try {
			return new SimpleDateFormat("yyyy-MM-dd").parse(date);
		}
		catch(ParseException e) {
			// keep trying below
		}

		try {
			return new SimpleDateFormat("MMM yyyy").parse(date);
		}
		catch(ParseException e) {
			// keep trying below
		}

		try {
			return new SimpleDateFormat("MMM d, yyyy").parse(date);
		}
		catch(ParseException e) {
			// keep trying below
		}

		throw new ParseException("Date string '" + date + "' was unparsable by all SimpleDateFormat attempts", 0);
	}

	public Date getDate(String xpath) throws XPathHelperException {
		String s = getString(xpath);
		try {
			return parseDate(s);
		}
		catch (Exception e) {
			throw new XPathHelperException("XPath '" + xpath + "' failed to return valid date: " + e.getMessage(), e);
		}
	}

	public Date getDate(String xpath, Date defaultValue) {
		try {
			return getDate(xpath);
		}
		catch (Exception e) {
			return defaultValue;
		}
	}

	public List getStrings(String xpath) throws XPathHelperException {
		try {
			List<String> strings = new ArrayList<String>();
			XPath path = XPath.newInstance(xpath);
      for (Namespace n : namespaces) path.addNamespace(n);
      Iterator itr = path.selectNodes(context).iterator();
			while (itr.hasNext()) {
				strings.add(((Text) itr.next()).getTextTrim());
			}
			return strings;
		}
		catch (JDOMException e) {
			throw new XPathHelperException("XPath '" + xpath + "' caused a JDOMException: " + e.getMessage(), e);
		}
	}

	public List<Element> getElements(String xpath) throws XPathHelperException {
		try {
			List<Element> elts = new ArrayList<Element>();
			XPath path = XPath.newInstance(xpath);
      for (Namespace n : namespaces) path.addNamespace(n);
      Iterator itr = path.selectNodes(context).iterator();
			while (itr.hasNext()) {
				elts.add((Element) itr.next());
			}
			return elts;
		}
		catch (JDOMException e) {
			throw new XPathHelperException("XPath '" + xpath + "' caused a JDOMException: " + e.getMessage(), e);
		}
	}

  public Element getElement(String xpath) throws XPathHelperException {
    try {
      XPath path = XPath.newInstance(xpath);
      for (Namespace n : namespaces) path.addNamespace(n);
      return (Element) path.selectSingleNode(context);
    }
    catch (JDOMException e) {
      throw new XPathHelperException("XPath '" + xpath + "' caused a JDOMException: " + e.getMessage(), e);
    }
  }

  public static String nullToEmpty(String str) {
		return str == null ? "" : str;
	}
}
