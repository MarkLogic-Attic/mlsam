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
 * Simple exception thrown by XPathHelper.  Should not be exposed to
 * the front end.  It should be caught by users of XPathHelper and
 * wrapped or handled appropriately there.
 */
class XPathHelperException extends Exception {
	public XPathHelperException() {
		super();
	}

	public XPathHelperException(String message) {
		super(message);
	}

	public XPathHelperException(String message, Throwable cause) {
		super(message, cause);
	}

	public XPathHelperException(Throwable cause) {
		super(cause);
	}
}
