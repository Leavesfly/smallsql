/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2006, by Volker Berlin.
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
 * CommandCreateView.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.command.ddl;

import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.jdbc.statement.SsStatement;
import io.leavesfly.smallsql.logger.Logger;
import io.leavesfly.smallsql.rdb.command.Command;
import io.leavesfly.smallsql.rdb.engine.table.Column;
import io.leavesfly.smallsql.rdb.engine.table.Columns;
import io.leavesfly.smallsql.rdb.sql.SQLParser;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;

public class CommandCreateView extends Command {

	private Columns columns = new Columns();
	public String sql;

	public CommandCreateView(Logger log, String name) {
		super(log);
		this.type = SQLTokenizer.VIEW;
		this.name = name;
	}

	/**
	 * Add a column definition. This is used from the SQLParser.
	 * 
	 * @see SQLParser#createTable
	 */
	void addColumn(Column column) {
		columns.add(column);
	}

	@Override
	public void executeImpl(SsConnection con, SsStatement st) throws Exception {
		con.getDatabase(false).createView(con, name, sql);
	}

}