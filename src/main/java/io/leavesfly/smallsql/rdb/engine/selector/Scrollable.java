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
 * Scrollable.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 05.07.2004
 */
package io.leavesfly.smallsql.rdb.engine.selector;

import io.leavesfly.smallsql.rdb.engine.RowSource;
import io.leavesfly.smallsql.util.datastruct.LongList;
import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;

/**
 * Scrollable is a RowSource wrapper to add the feature of scrollable to any
 * RowSource.
 * 
 * @author Volker Berlin
 */
public class Scrollable extends RowSource {

	/** The internal not scrollable RowSource */
	private final RowSource rowSource;

	/** The current row number */
	private int rowIdx;

	/** A map of row number to the RowPositions of the internal RowSource */
	private final LongList rowList = new LongList();

	/** Detect if on any previous scrolling the last row was reached. */
	public // private boolean lastIsFound;

	Scrollable(RowSource rowSource) {
		this.rowSource = rowSource;
	}

	public final boolean isScrollable() {
		return true;
	}

	public void beforeFirst() throws Exception {
		rowIdx = -1;
		rowSource.beforeFirst();
	}

	public boolean isBeforeFirst() {
		return rowIdx == -1 || rowList.size() == 0;
	}

	public boolean isFirst() {
		return rowIdx == 0 && rowList.size() > 0;
	}

	public boolean first() throws Exception {
		rowIdx = -1;
		return next();
	}

	public boolean previous() throws Exception {
		if (rowIdx > -1) {
			rowIdx--;
			if (rowIdx > -1 && rowIdx < rowList.size()) {
				rowSource.setRowPosition(rowList.get(rowIdx));
				return true;
			}
		}
		rowSource.beforeFirst();
		return false;
	}

	public boolean next() throws Exception {
		if (++rowIdx < rowList.size()) {
			rowSource.setRowPosition(rowList.get(rowIdx));
			return true;
		}
		final boolean result = rowSource.next();
		if (result) {
			rowList.add(rowSource.getRowPosition());
			return true;
		}
		rowIdx = rowList.size(); // rowIdx should be never larger as row count
		return false;
	}

	public boolean last() throws Exception {
		afterLast();
		return previous();
	}

	public boolean isLast() throws Exception {
		if (rowIdx + 1 != rowList.size()) {
			// there are more rows after the current row (rowIdx+1 <
			// rowList.size())
			// or we are after the last row (rowIdx+1 > rowList.size())
			return false;
		}
		boolean isNext = next();
		previous();
		return !isNext && (rowIdx + 1 == rowList.size() && rowList.size() > 0);
	}

	public boolean isAfterLast() throws Exception {
		if (rowIdx >= rowList.size())
			return true;
		if (isBeforeFirst() && rowList.size() == 0) {
			next();
			previous();
			if (rowList.size() == 0)
				return true;
		}
		return false;
	}

	public void afterLast() throws Exception {
		if (rowIdx + 1 < rowList.size()) {
			rowIdx = rowList.size() - 1;
			rowSource.setRowPosition(rowList.get(rowIdx));
		}
		while (next()) {/* scroll after the end */
		}
	}

	public boolean absolute(int row) throws Exception {
		if (row == 0)
			throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
		if (row < 0) {
			afterLast();
			rowIdx = rowList.size() + row;
			if (rowIdx < 0) {
				beforeFirst();
				return false;
			} else {
				rowSource.setRowPosition(rowList.get(rowIdx));
				return true;
			}
		}
		if (row <= rowList.size()) {
			rowIdx = row - 1;
			rowSource.setRowPosition(rowList.get(rowIdx));
			return true;
		}

		rowIdx = rowList.size() - 1;
		if (rowIdx >= 0)
			rowSource.setRowPosition(rowList.get(rowIdx));
		boolean result;
		while ((result = next()) && row - 1 > rowIdx) {/* scroll forward */
		}
		return result;
	}

	public boolean relative(int rows) throws Exception {
		int newRow = rows + rowIdx + 1;
		if (newRow <= 0) {
			beforeFirst();
			return false;
		} else {
			return absolute(newRow);
		}
	}

	public int getRow() throws Exception {
		if (rowIdx >= rowList.size())
			return 0;
		return rowIdx + 1;
	}

	public long getRowPosition() {
		return rowIdx;
	}

	public void setRowPosition(long rowPosition) throws Exception {
		rowIdx = (int) rowPosition;
	}

	public final boolean rowInserted() {
		return rowSource.rowInserted();
	}

	public final boolean rowDeleted() {
		return rowSource.rowDeleted();
	}

	public void nullRow() {
		rowSource.nullRow();
		rowIdx = -1;
	}

	public void noRow() {
		rowSource.noRow();
		rowIdx = -1;
	}

	public void execute() throws Exception {
		rowSource.execute();
		rowList.clear();
		rowIdx = -1;
	}

	/**
	 * @inheritDoc
	 */
	public boolean isExpressionsFromThisRowSource(Expressions columns) {
		return rowSource.isExpressionsFromThisRowSource(columns);
	}
}
