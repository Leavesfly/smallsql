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
 * Where.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 14.08.2004
 */
package org.yf.smallsql.rdb.engine.selector.multioper;

import org.yf.smallsql.rdb.engine.RowSource;
import org.yf.smallsql.rdb.sql.Expression;
import org.yf.smallsql.rdb.sql.expression.Expressions;

public class Where extends RowSource {

	final private RowSource rowSource;
	final private Expression where;
	private int row = 0;
	private boolean isCurrentRow;

	public Where(RowSource rowSource, Expression where) {
		this.rowSource = rowSource;
		this.where = where;
	}

	public RowSource getFrom() {
		return rowSource;
	}

	/**
	 * Verify if the valid row of the underlying RowSource (Variable join) is
	 * valid for the current ResultSet.
	 * 
	 * @return
	 */
	final private boolean isValidRow() throws Exception {
		return where == null || rowSource.rowInserted() || where.getBoolean();
	}

	public final boolean isScrollable() {
		return rowSource.isScrollable();
	}

	public final boolean isBeforeFirst() {
		return row == 0;
	}

	public final boolean isFirst() {
		return row == 1 && isCurrentRow;
	}

	public final boolean isLast() throws Exception {
		if (!isCurrentRow)
			return false;
		long rowPos = rowSource.getRowPosition();
		boolean isNext = next();
		rowSource.setRowPosition(rowPos);
		return !isNext;
	}

	public final boolean isAfterLast() {
		return row > 0 && !isCurrentRow;
	}

	public final void beforeFirst() throws Exception {
		rowSource.beforeFirst();
		row = 0;
	}

	public final boolean first() throws Exception {
		isCurrentRow = rowSource.first();
		while (isCurrentRow && !isValidRow()) {
			isCurrentRow = rowSource.next();
		}
		row = 1;
		return isCurrentRow;
	}

	public final boolean previous() throws Exception {
		boolean oldIsCurrentRow = isCurrentRow;
		do {
			isCurrentRow = rowSource.previous();
		} while (isCurrentRow && !isValidRow());
		if (oldIsCurrentRow || isCurrentRow)
			row--;
		return isCurrentRow;
	}

	public final boolean next() throws Exception {
		boolean oldIsCurrentRow = isCurrentRow;
		do {
			isCurrentRow = rowSource.next();
		} while (isCurrentRow && !isValidRow());
		if (oldIsCurrentRow || isCurrentRow)
			row++;
		return isCurrentRow;
	}

	public final boolean last() throws Exception {
		while (next()) {/* scroll after the end */
		}
		return previous();
	}

	public final void afterLast() throws Exception {
		while (next()) {/* scroll after the end */
		}
	}

	public final int getRow() throws Exception {
		return isCurrentRow ? row : 0;
	}

	public final long getRowPosition() {
		return rowSource.getRowPosition();
	}

	public final void setRowPosition(long rowPosition) throws Exception {
		rowSource.setRowPosition(rowPosition);
	}

	public final void nullRow() {
		rowSource.nullRow();
		row = 0;
	}

	public final void noRow() {
		rowSource.noRow();
		row = 0;
	}

	public final boolean rowInserted() {
		return rowSource.rowInserted();
	}

	public final boolean rowDeleted() {
		return rowSource.rowDeleted();
	}

	public final void execute() throws Exception {
		rowSource.execute();
	}

	/**
	 * @inheritDoc
	 */
	public boolean isExpressionsFromThisRowSource(Expressions columns) {
		return rowSource.isExpressionsFromThisRowSource(columns);
	}
}
