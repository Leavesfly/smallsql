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
 * Command.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.rdb.command;

import java.sql.*;

import org.yf.smallsql.jdbc.SmallSQLException;
import org.yf.smallsql.jdbc.SsConnection;
import org.yf.smallsql.jdbc.SsResultSet;
import org.yf.smallsql.jdbc.statement.SsStatement;
import org.yf.smallsql.lang.Language;
import org.yf.smallsql.logger.Logger;
import org.yf.smallsql.rdb.sql.Expression;
import org.yf.smallsql.rdb.sql.SQLParser;
import org.yf.smallsql.rdb.sql.expression.ExpressionValue;
import org.yf.smallsql.rdb.sql.expression.Expressions;

public abstract class Command {

	protected int type;
	protected String catalog;
	public String name;

	public SsResultSet rs;
	protected int updateCount = -1;

	/** List of Columns */
	public final Expressions columnExpressions;

	/** List of ExpressionValue */
	public Expressions params = new Expressions();

	protected final Logger log;

	public Command(Logger log) {
		this.log = log;
		this.columnExpressions = new Expressions();
	}

	public Command(Logger log, Expressions columnExpressions) {
		this.log = log;
		this.columnExpressions = columnExpressions;
	}

	/**
	 * Add a Expression that returns the value for Column. This method is used
	 * from SQLParser for different Commands (CommandSelect, CommandInsert).
	 * 
	 * @see SQLParser#select()
	 * @see SQLParser#insert()
	 */
	public void addColumnExpression(Expression column) throws SQLException {
		columnExpressions.add(column);
	}

	public void addParameter(ExpressionValue param) {
		params.add(param);
	}

	/**
	 * check if all parameters are set
	 */
	public void verifyParams() throws SQLException {
		for (int p = 0; p < params.size(); p++) {
			if (((ExpressionValue) params.get(p)).isEmpty())
				throw SmallSQLException.create(Language.PARAM_EMPTY, new Integer(p + 1));
		}
	}

	/**
	 * Clear all parameters of a PreparedStatement
	 */
	public void clearParams() {
		for (int p = 0; p < params.size(); p++) {
			((ExpressionValue) params.get(p)).clear();
		}
	}

	/**
	 * Get a PreparedStatement parameter. The idx starts with 1.
	 */
	private ExpressionValue getParam(int idx) throws SQLException {
		if (idx < 1 || idx > params.size())
			throw SmallSQLException.create(Language.PARAM_IDX_OUT_RANGE, new Object[] { new Integer(idx),
					new Integer(params.size()) });
		return ((ExpressionValue) params.get(idx - 1));
	}

	/**
	 * Set value of a PreparedStatement parameter. The idx starts with 1.
	 */
	public void setParamValue(int idx, Object value, int dataType) throws SQLException {
		getParam(idx).set(value, dataType);
		if (log.isLogging()) {
			log.println("param" + idx + '=' + value + "; type=" + dataType);
		}
	}

	/**
	 * Set value of a PreparedStatement parameter. The idx starts with 1.
	 */
	public void setParamValue(int idx, Object value, int dataType, int length) throws SQLException {
		getParam(idx).set(value, dataType, length);
		if (log.isLogging()) {
			log.println("param" + idx + '=' + value + "; type=" + dataType + "; length=" + length);
		}
	}

	public final void execute(SsConnection con, SsStatement st) throws SQLException {
		int savepoint = con.getSavepoint();
		try {
			executeImpl(con, st);
		} catch (Throwable e) {
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		} finally {
			if (con.getAutoCommit())
				con.commit();
		}
	}

	public abstract void executeImpl(SsConnection con, SsStatement st) throws Exception;

	public SsResultSet getQueryResult() throws SQLException {
		if (rs == null)
			throw SmallSQLException.create(Language.RSET_NOT_PRODUCED);
		return rs;
	}

	public SsResultSet getResultSet() {
		return rs;
	}

	public int getUpdateCount() {
		return updateCount;
	}

	/**
	 * The default Command remove all results because there is only one result.
	 * 
	 * @return ever false
	 */
	public boolean getMoreResults() {
		rs = null;
		updateCount = -1;
		return false;
	}

	/**
	 * Set the max rows. Need to be override in the Commands that support it.
	 */
	public void setMaxRows(int max) {
		/*
		 * Empty because not supported for the most Commands
		 */
	}

	public int getMaxRows() {
		return -1;
	}
}