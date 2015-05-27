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
 * RowSource.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.rdb.engine;

import java.sql.*;

import org.yf.smallsql.jdbc.SmallSQLException;
import org.yf.smallsql.lang.Language;
import org.yf.smallsql.rdb.sql.expression.Expressions;
import org.yf.smallsql.util.Utils;

/**
 * This is the navigation through the rows of any source. This is an abstract
 * class and not an interface because interfaces are ever public. All
 * Implementations are used as a Source in the FROM clause.
 * 
 * Know Implementations are: - Join - TableResult - ViewResult - MemoryResult -
 * GroupResult (extends from MemoryResult) - SortedResult - UnionAll
 * 
 * Future Implementations are: - Inner SELECT - row function
 */
public abstract class RowSource {

	/**
	 * If this RowSource is scrollable. It means it can scroll in all
	 * directions.
	 */
	public abstract boolean isScrollable();

	/**
	 * Equals to ResultSet.beforeFirst()
	 */
	public abstract void beforeFirst() throws Exception;

	/**
	 * Equals to ResultSet.isBeforeFirst().
	 */
	public boolean isBeforeFirst() throws SQLException {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	/**
	 * Equals to ResultSet.isFirst().
	 */
	public boolean isFirst() throws SQLException {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	/**
	 * Equals to ResultSet.first()
	 * 
	 * @return
	 */
	public abstract boolean first() throws Exception;

	public boolean previous() throws Exception {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	public abstract boolean next() throws Exception;

	public boolean last() throws Exception {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	/**
	 * Equals to ResultSet.isLast().
	 */
	public boolean isLast() throws Exception {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	/**
	 * Equals to ResultSet.isAfterLast().
	 */
	public boolean isAfterLast() throws SQLException, Exception {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	public abstract void afterLast() throws Exception;

	public boolean absolute(int row) throws Exception {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	public boolean relative(int rows) throws Exception {
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}

	public abstract int getRow() throws Exception;

	/**
	 * Get a marker for the current row. The method setRowPostion must be
	 * reconstruct the current row. The RowPosition can be a file offset for
	 * TableResult. This is using for SortedResult.
	 * 
	 * @return The value need be >= 0. It can be a counter for MemoryResult.
	 */
	public abstract long getRowPosition();

	/**
	 * Restore the row that was marked with the value. This is using for
	 * SortedResult.
	 * 
	 * @param rowPosition
	 *            Only values that are return from getRowPosition are valid.
	 */
	public abstract void setRowPosition(long rowPosition) throws Exception;

	/**
	 * Is used for OUTER JOIN to set the RowSource to NULL if the row exists
	 * only in the major RowSource (table)
	 */
	public abstract void nullRow();

	/**
	 * Is used for JOIN to set both site to "No current row". This is needed if
	 * one site has 0 rows that the getXXX() methods throw this exception.
	 */
	public abstract void noRow();

	/**
	 * If the current row is inserted in this ResultSet.
	 */
	public abstract boolean rowInserted();

	/**
	 * If the current row is deleted.
	 */
	public abstract boolean rowDeleted();

	/**
	 * Returns true if a alias was set and no more alias can be set. This is
	 * used from the SQLParser
	 * 
	 * @return
	 */
	public boolean hasAlias() {
		return true;
	}

	public void setAlias(String name) throws SQLException {
		throw SmallSQLException.create(Language.ALIAS_UNSUPPORTED);
	}

	/**
	 * Perform some operation on some RowSources per ResultSet. For example the
	 * grouping on GroupResult and sorting on SortedResult.
	 */
	public abstract void execute() throws Exception;

	/**
	 * Check if the list of ExpressionName based on this RowSource.
	 * 
	 * @param columns
	 *            list of ExpressionNames
	 * @return false if one or more ExpressionName that not based on this
	 *         RowSource
	 * @see Utils#getExpressionNameFromTree(Expression)
	 */
	public abstract boolean isExpressionsFromThisRowSource(Expressions columns);

}