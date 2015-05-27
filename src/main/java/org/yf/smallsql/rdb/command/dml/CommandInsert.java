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
 * CommandInsert.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.rdb.command.dml;

import java.sql.SQLException;
import java.util.ArrayList;

import org.yf.smallsql.jdbc.SmallSQLException;
import org.yf.smallsql.jdbc.SsConnection;
import org.yf.smallsql.jdbc.SsResultSet;
import org.yf.smallsql.jdbc.statement.SsStatement;
import org.yf.smallsql.lang.Language;
import org.yf.smallsql.logger.Logger;
import org.yf.smallsql.rdb.command.Command;
import org.yf.smallsql.rdb.command.dql.CommandSelect;
import org.yf.smallsql.rdb.engine.Table;
import org.yf.smallsql.rdb.engine.View;
import org.yf.smallsql.rdb.engine.index.IndexDescriptions;
import org.yf.smallsql.rdb.engine.store.StoreImpl;
import org.yf.smallsql.rdb.engine.table.Column;
import org.yf.smallsql.rdb.sql.Expression;
import org.yf.smallsql.rdb.sql.datatype.Strings;
import org.yf.smallsql.rdb.sql.expression.Expressions;
import org.yf.smallsql.util.Utils;

public class CommandInsert extends Command {

	public boolean noColumns; // after the table name was no columnExpressions;
								// all columnExpressions in default order
	private CommandSelect cmdSel;

	private Table table;
	private long tableTimestamp;
	private int[] matrix; // mapping of the columns from INSERT to the columns
							// in the Table; -1 default Value

	public CommandInsert(Logger log, String name) {
		super(log);
		this.name = name;
	}

	public void addColumnExpression(Expression column) throws SQLException {
		if (columnExpressions.indexOf(column) >= 0) {
			throw SmallSQLException.create(Language.COL_DUPLICATE, column);
		}
		super.addColumnExpression(column);
	}

	public void addValues(Expressions values) {
		// this.values = values;
		this.cmdSel = new CommandSelect(log, values);
	}

	public void addValues(CommandSelect cmdSel) {
		this.cmdSel = cmdSel;
	}

	/**
	 * The method compile set all needed reference links after the Parsing
	 */
	private void compile(SsConnection con) throws Exception {
		View tableView = con.getDatabase(false).getTableView(con, name);
		if (!(tableView instanceof Table))
			throw SmallSQLException.create(Language.VIEW_INSERT);
		table = (Table) tableView;
		tableTimestamp = table.getTimestamp();
		cmdSel.compile(con);
		int count = table.columns.size();
		matrix = new int[count];
		if (noColumns) {
			// noColumns means a table without Columns like INSERT INTO mytable
			// VALUES(1,2)
			// in this case all columnExpressions of the table need to use
			columnExpressions.clear();
			for (int i = 0; i < count; i++) {
				matrix[i] = i;
			}
			if (count != cmdSel.columnExpressions.size())
				throw SmallSQLException.create(Language.COL_VAL_UNMATCH);
		} else {
			for (int i = 0; i < count; i++)
				matrix[i] = -1;
			for (int c = 0; c < columnExpressions.size(); c++) {
				// listing of the column names in the INSERT SQL expression
				Expression sqlCol = columnExpressions.get(c);
				String sqlColName = sqlCol.getName();
				int idx = table.findColumnIdx(sqlColName);
				if (idx >= 0) {
					matrix[idx] = c;
				} else {
					throw SmallSQLException.create(Language.COL_MISSING, sqlColName);
				}
			}
			if (columnExpressions.size() != cmdSel.columnExpressions.size())
				throw SmallSQLException.create(Language.COL_VAL_UNMATCH);
		}
	}

	public void executeImpl(SsConnection con, SsStatement st) throws Exception {
		// on first time and on change of the table we need to recompile
		if (table == null || tableTimestamp != table.getTimestamp())
			compile(con);

		final IndexDescriptions indexes = table.indexes;

		updateCount = 0;
		cmdSel.from.execute();
		cmdSel.beforeFirst();

		// Variables for GeneratedKeys
		Strings keyColumnNames = null;
		ArrayList<Object> keys = null;
		boolean needGeneratedKeys = st.needGeneratedKeys();
		int generatedKeysType = 0;

		while (cmdSel.next()) {
			if (needGeneratedKeys) {
				keyColumnNames = new Strings();
				keys = new ArrayList<Object>();
				if (st.getGeneratedKeyNames() != null)
					generatedKeysType = 1;
				if (st.getGeneratedKeyIndexes() != null)
					generatedKeysType = 2;
			}
			StoreImpl store = table.getStoreInsert(con);
			for (int c = 0; c < matrix.length; c++) {
				Column column = table.columns.get(c);
				int idx = matrix[c];
				Expression valueExpress;
				if (idx >= 0) {
					valueExpress = cmdSel.columnExpressions.get(idx);
				} else {
					valueExpress = column.getDefaultValue(con);
					if (needGeneratedKeys && generatedKeysType == 0 && valueExpress != Expression.NULL) {
						keyColumnNames.add(column.getName());
						keys.add(valueExpress.getObject());
					}
				}
				if (needGeneratedKeys && generatedKeysType == 1) {
					String[] keyNames = st.getGeneratedKeyNames();
					for (int i = 0; i < keyNames.length; i++) {
						if (column.getName().equalsIgnoreCase(keyNames[i])) {
							keyColumnNames.add(column.getName());
							keys.add(valueExpress.getObject());
							break;
						}
					}
				}
				if (needGeneratedKeys && generatedKeysType == 2) {
					int[] keyIndexes = st.getGeneratedKeyIndexes();
					for (int i = 0; i < keyIndexes.length; i++) {
						if (c + 1 == keyIndexes[i]) {
							keyColumnNames.add(column.getName());
							keys.add(valueExpress.getObject());
							break;
						}
					}
				}
				store.writeExpression(valueExpress, column);
				for (int i = 0; i < indexes.size(); i++) {
					indexes.get(i).writeExpression(c, valueExpress);
				}
			}
			store.writeFinsh(con);
			for (int i = 0; i < indexes.size(); i++) {
				indexes.get(i).writeFinish(con);
			}
			updateCount++;
			if (needGeneratedKeys) {
				Object[][] data = new Object[1][keys.size()];
				keys.toArray(data[0]);
				st.setGeneratedKeys(new SsResultSet(st, Utils.createMemoryCommandSelect(con, keyColumnNames.toArray(),
						data)));
			}
		}
	}

}