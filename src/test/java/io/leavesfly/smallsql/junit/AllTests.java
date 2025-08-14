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
package io.leavesfly.smallsql.junit;

import io.leavesfly.smallsql.junit.sql.TestExceptionMethods;
import io.leavesfly.smallsql.junit.sql.TestLanguage;
import io.leavesfly.smallsql.junit.sql.TestTokenizer;
import io.leavesfly.smallsql.junit.sql.ddl.TestAlterTable2;
import io.leavesfly.smallsql.junit.sql.ddl.TestDBMetaData;
import io.leavesfly.smallsql.junit.sql.ddl.TestIdentifer;
import io.leavesfly.smallsql.junit.sql.dml.TestDeleteUpdate;
import io.leavesfly.smallsql.junit.sql.dql.TestGroupBy;
import io.leavesfly.smallsql.junit.sql.dql.TestMoneyRounding;
import io.leavesfly.smallsql.junit.sql.dql.TestScrollable;
import io.leavesfly.smallsql.junit.sql.dql.TestStatement;
import junit.framework.*;
import junit.textui.TestRunner;

import java.sql.*;
import java.util.Properties;

import io.leavesfly.smallsql.junit.sql.TestThreads;
import io.leavesfly.smallsql.junit.sql.ddl.TestAlterTable;
import io.leavesfly.smallsql.junit.sql.ddl.TestDataTypes;
import io.leavesfly.smallsql.junit.sql.ddl.TestOther;
import io.leavesfly.smallsql.junit.sql.dql.TestExceptions;
import io.leavesfly.smallsql.junit.sql.dql.TestFunctions;
import io.leavesfly.smallsql.junit.sql.dql.TestJoins;
import io.leavesfly.smallsql.junit.sql.dql.TestOperatoren;
import io.leavesfly.smallsql.junit.sql.dql.TestOrderBy;
import io.leavesfly.smallsql.junit.sql.dql.TestResultSet;
import io.leavesfly.smallsql.junit.sql.tpl.TestTransactions;

/**
 * 测试套件主类，用于运行SmallSQL数据库的所有单元测试。
 * 该类负责管理数据库连接并组织所有测试用例。
 */
public class AllTests extends TestCase {

	public final static String CATALOG = "testdata";
	public final static String JDBC_URL = "jdbc:smallsql:" + CATALOG;
	private static Connection con;
	
	static {
		try {
			Class.forName("smallsql.jdbc.SSDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to load SmallSQL driver", e);
		}
	}

	/**
	 * 获取数据库连接实例。
	 * 如果当前没有连接或连接已关闭，则创建一个新的连接。
	 * 
	 * @return 数据库连接对象
	 * @throws SQLException 当数据库访问出错时抛出
	 */
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
		return DriverManager.getConnection(urlComplete, info);
	}

	/**
	 * 打印结果集中的所有数据。
	 * 遍历结果集的每一行，并将每列的值以制表符分隔的形式打印到控制台。
	 * 
	 * @param rs 要打印的结果集
	 * @throws SQLException 当访问结果集出现错误时抛出
	 */
	public static void printRS(ResultSet rs) throws SQLException {
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(rs.getObject(i) + "\t");
			}
			System.out.println();
		}
	}

	/**
	 * 创建并返回包含所有测试用例的测试套件。
	 * 
	 * @return 包含所有测试用例的TestSuite对象
	 * @throws Exception 当创建测试套件出现问题时抛出
	 */
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

	/**
	 * 程序入口点，运行所有测试用例。
	 * 
	 * @param argv 命令行参数
	 */
	public static void main(String[] argv) {
		try {
			TestRunner.main(new String[] { AllTests.class.getName() });
		} catch (Exception e) {
			System.err.println("Test execution failed: " + e.getMessage());
			e.printStackTrace();
		}
	}

}