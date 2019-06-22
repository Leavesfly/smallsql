/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2007, by Volker Berlin.
 *
 * Project Info:  http://www.smallsql.de/
 *
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by 
 * the Free Software Foundation; either version 2.1 of the License, or 
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but 
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, 
 * USA.  
 *
 * [Java is a trademark or registered trademark of Sun Microsystems, Inc. 
 * in the United States and other countries.]
 *
 * ---------------
 * SSDriver.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql;

import java.sql.*;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.lang.Language;

public class SsDriver implements Driver {

	public static final String URL_PREFIX = "jdbc:smallsql";

	public static SsDriver DRIVER;
	static {
		try {
			DRIVER = new SsDriver();
			DriverManager.registerDriver(DRIVER);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}
		return new SsConnection(parse(url, info));
	}

	/**
	 * Parsed the JDBC URL and build together
	 * 
	 * @param url
	 *            the JDBC URL
	 * @param info
	 *            a list of arbitrary properties
	 * @return a new Properties object
	 */
	private Properties parse(String url, Properties info) throws SQLException {
		Properties props = (Properties) info.clone();
		if (!acceptsURL(url)) {
			return props;
		}
		int idx1 = url.indexOf(':', 5); // search after "jdbc:"
		int idx2 = url.indexOf('?');
		if (idx1 > 0) {
			String dbPath = (idx2 > 0) ? url.substring(idx1 + 1, idx2) : url.substring(idx1 + 1);
			props.setProperty("dbpath", dbPath);
		}
		if (idx2 > 0) {
			String propsString = url.substring(idx2 + 1).replace('&', ';');
			StringTokenizer tok = new StringTokenizer(propsString, ";");
			while (tok.hasMoreTokens()) {
				String keyValue = tok.nextToken().trim();
				if (keyValue.length() > 0) {
					idx1 = keyValue.indexOf('=');
					if (idx1 > 0) {
						String key = keyValue.substring(0, idx1).toLowerCase().trim();
						String value = keyValue.substring(idx1 + 1).trim();
						props.put(key, value);
					} else {
						throw SmallSQLException
								.create(Language.CUSTOM_MESSAGE, "Missing equal in property:" + keyValue);
					}
				}
			}
		}
		return props;
	}

	@Override
	public boolean acceptsURL(String url) {
		return url.startsWith(URL_PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		Properties props = parse(url, info);
		DriverPropertyInfo[] driverInfos = new DriverPropertyInfo[1];
		driverInfos[0] = new DriverPropertyInfo("dbpath", props.getProperty("dbpath"));
		return driverInfos;
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 21;
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO
		return null;
	}
}