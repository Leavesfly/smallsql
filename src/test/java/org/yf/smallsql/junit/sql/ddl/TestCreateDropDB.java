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
 * TestOthers.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 31.07.2004
 */
package org.yf.smallsql.junit.sql.ddl;

import java.sql.*;

import org.yf.smallsql.junit.BasicTestCase;

/**
 * @author Volker Berlin
 */
public class TestCreateDropDB extends BasicTestCase {

	public void testCreateDropDatabases() throws Exception {
		try {
			Class.forName("smallsql.jdbc.SSDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection con = DriverManager.getConnection("jdbc:smallsql");

		Statement st = con.createStatement();
		try {
			st.execute("Create Database anyTestDatabase");
		} catch (SQLException ex) {
			st.execute("Drop Database anyTestDatabase");
			throw ex;
		}
		st.execute("Drop Database anyTestDatabase");
	}

}
