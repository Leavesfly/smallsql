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
 * AllTests.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.junit;

import junit.framework.*;
import junit.textui.TestRunner;

import java.sql.*;
import java.util.Properties;

import org.yf.smallsql.junit.sql.TestExceptionMethods;
import org.yf.smallsql.junit.sql.TestLanguage;
import org.yf.smallsql.junit.sql.TestThreads;
import org.yf.smallsql.junit.sql.TestTokenizer;
import org.yf.smallsql.junit.sql.ddl.TestAlterTable;
import org.yf.smallsql.junit.sql.ddl.TestAlterTable2;
import org.yf.smallsql.junit.sql.ddl.TestDBMetaData;
import org.yf.smallsql.junit.sql.ddl.TestDataTypes;
import org.yf.smallsql.junit.sql.ddl.TestIdentifer;
import org.yf.smallsql.junit.sql.ddl.TestOther;
import org.yf.smallsql.junit.sql.dml.TestDeleteUpdate;
import org.yf.smallsql.junit.sql.dql.TestExceptions;
import org.yf.smallsql.junit.sql.dql.TestFunctions;
import org.yf.smallsql.junit.sql.dql.TestGroupBy;
import org.yf.smallsql.junit.sql.dql.TestJoins;
import org.yf.smallsql.junit.sql.dql.TestMoneyRounding;
import org.yf.smallsql.junit.sql.dql.TestOperatoren;
import org.yf.smallsql.junit.sql.dql.TestOrderBy;
import org.yf.smallsql.junit.sql.dql.TestResultSet;
import org.yf.smallsql.junit.sql.dql.TestScrollable;
import org.yf.smallsql.junit.sql.dql.TestStatement;
import org.yf.smallsql.junit.sql.tpl.TestTransactions;

public class AllTests extends TestCase {

	public final static String CATALOG = "testdata";
	public final static String JDBC_URL = "jdbc:smallsql:" + CATALOG;
	private static Connection con;

	public static Connection getConnection() throws SQLException {
		if (con == null || con.isClosed()) {
			con = createConnection();
		}
		return con;
	}

	/**
	 * Creates a connection in the English locale.<br>
	 */
	public static Connection createConnection() throws SQLException {
		// DriverManager.setLogStream( System.out );
		try {
			Class.forName("smallsql.jdbc.SSDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return DriverManager.getConnection(JDBC_URL + "?create=true;locale=en");
	}

	/**
	 * Creates a connection, with the possibility of appending an additional
	 * string to the url and/or passing a Properties object.<br>
	 * Locale is not specified.
	 * 
	 * @param urlAddition
	 *            String to append to url; nullable.
	 * @param info
	 *            object Properties; nullable.
	 * @return connection created.
	 */
	public static Connection createConnection(String urlAddition, Properties info) throws SQLException {

		if (urlAddition == null)
			urlAddition = "";
		if (info == null)
			info = new Properties();

		String urlComplete = JDBC_URL + urlAddition;
		try {
			Class.forName("smallsql.jdbc.SSDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return DriverManager.getConnection(urlComplete, info);
	}

	public static void printRS(ResultSet rs) throws SQLException {
		while (rs.next()) {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				System.out.print(rs.getObject(i) + "\t");
			}
			System.out.println();
		}
	}

	public static Test suite() throws Exception {
		TestSuite theSuite = new TestSuite("SmallSQL all Tests");
		theSuite.addTestSuite(TestOther.class);

		theSuite.addTestSuite(TestAlterTable.class);
		theSuite.addTestSuite(TestAlterTable2.class);
		theSuite.addTest(TestDataTypes.suite());
		theSuite.addTestSuite(TestDBMetaData.class);
		theSuite.addTestSuite(TestExceptionMethods.class);
		theSuite.addTest(TestExceptions.suite());
		theSuite.addTestSuite(TestDeleteUpdate.class);
		theSuite.addTest(TestFunctions.suite());
		theSuite.addTestSuite(TestGroupBy.class);
		theSuite.addTestSuite(TestIdentifer.class);
		theSuite.addTest(TestJoins.suite());
		theSuite.addTestSuite(TestLanguage.class);
		theSuite.addTestSuite(TestMoneyRounding.class);
		theSuite.addTest(TestOperatoren.suite());
		theSuite.addTestSuite(TestOrderBy.class);
		theSuite.addTestSuite(TestResultSet.class);
		theSuite.addTestSuite(TestScrollable.class);
		theSuite.addTestSuite(TestStatement.class);
		theSuite.addTestSuite(TestThreads.class);
		theSuite.addTestSuite(TestTokenizer.class);
		theSuite.addTestSuite(TestTransactions.class);
		return theSuite;
	}

	public static void main(String[] argv) {
		try {

			TestRunner.main(new String[] { AllTests.class.getName() });
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}