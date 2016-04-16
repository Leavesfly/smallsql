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
 * ViewResult.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 05.06.2004
 */
package io.leavesfly.smallsql.rdb.engine.selector.result;

import java.sql.*;

import io.leavesfly.smallsql.rdb.sql.Expression;
import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.rdb.command.dql.CommandSelect;
import io.leavesfly.smallsql.rdb.engine.View;
import io.leavesfly.smallsql.rdb.engine.ViewTable;
import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;

/**
 * @author Volker Berlin
 */
public class ViewResult extends TableViewResult {

	final private ViewTable view;
	final private Expressions columnExpressions;
	final private CommandSelect commandSelect;

	public ViewResult(ViewTable view) {
		this.view = view;
		this.columnExpressions = view.commandSelect.columnExpressions;
		this.commandSelect = view.commandSelect;
	}

	/**
	 * Constructor is used for UNION
	 * 
	 * @throws Exception
	 * 
	 */
	public ViewResult(SsConnection con, CommandSelect commandSelect) throws SQLException {
		try {
			this.view = new ViewTable(con, commandSelect);
			this.columnExpressions = commandSelect.columnExpressions;
			this.commandSelect = commandSelect;
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
	}

	/**
	 * Is used for compile() of different Commands
	 * 
	 * @param con
	 * @return true if now init; false if already init
	 * @throws Exception
	 */
	public boolean init(SsConnection con) throws Exception {
		if (super.init(con)) {
			commandSelect.compile(con);
			return true;
		}
		return false;
	}

	/*
	 * =====================================================================
	 * 
	 * Methods of base class TableViewResult
	 * 
	 * ====================================================================
	 */

	public View getTableView() {
		return view;
	}

	public void deleteRow() throws SQLException {
		commandSelect.deleteRow(con);
	}

	public void updateRow(Expression[] updateValues) throws Exception {
		commandSelect.updateRow(con, updateValues);
	}

	public void insertRow(Expression[] updateValues) throws Exception {
		commandSelect.insertRow(con, updateValues);
	}

	/*
	 * =====================================================================
	 * 
	 * Methods of interface DataSource
	 * 
	 * ====================================================================
	 */
	public boolean isNull(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).isNull();
	}

	public boolean getBoolean(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getBoolean();
	}

	public int getInt(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getInt();
	}

	public long getLong(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getLong();
	}

	public float getFloat(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getFloat();
	}

	public double getDouble(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getDouble();
	}

	public long getMoney(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getMoney();
	}

	public MutableNumeric getNumeric(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getNumeric();
	}

	public Object getObject(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getObject();
	}

	public String getString(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getString();
	}

	public byte[] getBytes(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getBytes();
	}

	public int getDataType(int colIdx) {
		return columnExpressions.get(colIdx).getDataType();
	}

	/*
	 * =====================================================
	 * 
	 * Methods of the interface RowSource
	 * 
	 * =====================================================
	 */

	public void beforeFirst() throws Exception {
		commandSelect.beforeFirst();
	}

	public boolean isBeforeFirst() throws SQLException {
		return commandSelect.isBeforeFirst();
	}

	public boolean isFirst() throws SQLException {
		return commandSelect.isFirst();
	}

	public boolean first() throws Exception {
		return commandSelect.first();
	}

	public boolean previous() throws Exception {
		return commandSelect.previous();
	}

	public boolean next() throws Exception {
		return commandSelect.next();
	}

	public boolean last() throws Exception {
		return commandSelect.last();
	}

	public boolean isLast() throws Exception {
		return commandSelect.isLast();
	}

	public boolean isAfterLast() throws Exception {
		return commandSelect.isAfterLast();
	}

	public void afterLast() throws Exception {
		commandSelect.afterLast();
	}

	public boolean absolute(int row) throws Exception {
		return commandSelect.absolute(row);
	}

	public boolean relative(int rows) throws Exception {
		return commandSelect.relative(rows);
	}

	public int getRow() throws Exception {
		return commandSelect.getRow();
	}

	public long getRowPosition() {
		return commandSelect.from.getRowPosition();
	}

	public void setRowPosition(long rowPosition) throws Exception {
		commandSelect.from.setRowPosition(rowPosition);
	}

	public final boolean rowInserted() {
		return commandSelect.from.rowInserted();
	}

	public final boolean rowDeleted() {
		return commandSelect.from.rowDeleted();
	}

	public void nullRow() {
		commandSelect.from.nullRow();

	}

	public void noRow() {
		commandSelect.from.noRow();
	}

	public final void execute() throws Exception {
		commandSelect.from.execute();
	}
}
