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
 * SortedResult.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.rdb.engine.selector.multioper;

import org.yf.smallsql.jdbc.SmallSQLException;
import org.yf.smallsql.lang.Language;
import org.yf.smallsql.rdb.engine.Index;
import org.yf.smallsql.rdb.engine.IndexScrollStatus;
import org.yf.smallsql.rdb.engine.RowSource;
import org.yf.smallsql.rdb.sql.expression.Expressions;
import org.yf.smallsql.util.datastruct.LongList;

/**
 * Is used to implements the ORDER BY clause.
 * 
 * @author Volker Berlin
 */
public final class SortedResult extends RowSource {

	final private Expressions orderBy;
	/**
	 * The underlying RowSource that should be sorted.
	 */
	final private RowSource rowSource;
	/**
	 * Scroll pointer to the index.
	 */
	private IndexScrollStatus scrollStatus;
	/**
	 * The current row number. It is used for getRow().
	 */
	private int row;
	/**
	 * Added (inserted) rows if it is an updatable ResultSet.
	 */
	private final LongList insertedRows = new LongList();
	private boolean useSetRowPosition;
	/**
	 * The count of rows in the sorted index. This is the count without inserted
	 * rows.
	 */
	private int sortedRowCount;
	/**
	 * The rowOffset of the original RowSource (rowSource) that was read in the
	 * SortedResult. This can be the last row of the original RowSource. But
	 * there can be also insert new row after it.
	 */
	private long lastRowOffset;

	public SortedResult(RowSource rowSource, Expressions orderBy) {
		this.rowSource = rowSource;
		this.orderBy = orderBy;
	}

	public final boolean isScrollable() {
		return true;
	}

	public final void execute() throws Exception {
		rowSource.execute();
		Index index = new Index(false);
		lastRowOffset = -1;
		while (rowSource.next()) {
			lastRowOffset = rowSource.getRowPosition();
			index.addValues(lastRowOffset, orderBy);
			sortedRowCount++;
		}
		scrollStatus = index.createScrollStatus(orderBy);
		useSetRowPosition = false;
	}

	public final boolean isBeforeFirst() {
		return row == 0;
	}

	public final boolean isFirst() {
		return row == 1;
	}

	public void beforeFirst() throws Exception {
		scrollStatus.reset();
		row = 0;
		useSetRowPosition = false;
	}

	public boolean first() throws Exception {
		beforeFirst();
		return next();
	}

	public boolean previous() throws Exception {
		if (useSetRowPosition)
			throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
		if (currentInsertedRow() == 0) {
			scrollStatus.afterLast();
		}
		row--;
		if (currentInsertedRow() >= 0) {
			rowSource.setRowPosition(insertedRows.get(currentInsertedRow()));
			return true;
		}
		long rowPosition = scrollStatus.getRowOffset(false);
		if (rowPosition >= 0) {
			rowSource.setRowPosition(rowPosition);
			return true;
		} else {
			rowSource.noRow();
			row = 0;
			return false;
		}
	}

	public boolean next() throws Exception {
		if (useSetRowPosition)
			throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
		if (currentInsertedRow() < 0) {
			long rowPosition = scrollStatus.getRowOffset(true);
			if (rowPosition >= 0) {
				row++;
				rowSource.setRowPosition(rowPosition);
				return true;
			}
		}
		if (currentInsertedRow() < insertedRows.size() - 1) {
			row++;
			rowSource.setRowPosition(insertedRows.get(currentInsertedRow()));
			return true;
		}
		if (lastRowOffset >= 0) {
			rowSource.setRowPosition(lastRowOffset);
		} else {
			rowSource.beforeFirst();
		}
		if (rowSource.next()) {
			row++;
			lastRowOffset = rowSource.getRowPosition();
			insertedRows.add(lastRowOffset);
			return true;
		}
		rowSource.noRow();
		row = (getRowCount() > 0) ? getRowCount() + 1 : 0;
		return false;
	}

	public boolean last() throws Exception {
		afterLast();
		return previous();
	}

	public final boolean isLast() throws Exception {
		if (row == 0) {
			return false;
		}
		if (row > getRowCount()) {
			return false;
		}
		boolean isNext = next();
		previous();
		return !isNext;
	}

	public final boolean isAfterLast() {
		int rowCount = getRowCount();
		return row > rowCount || rowCount == 0;
	}

	public void afterLast() throws Exception {
		useSetRowPosition = false;
		if (sortedRowCount > 0) {
			scrollStatus.afterLast();
			scrollStatus.getRowOffset(false); // previous position
		} else {
			rowSource.beforeFirst();
		}
		row = sortedRowCount;
		while (next()) {
			// scroll to the end if there inserted rows
		}
	}

	public boolean absolute(int newRow) throws Exception {
		if (newRow == 0)
			throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
		if (newRow > 0) {
			beforeFirst();
			while (newRow-- > 0) {
				if (!next()) {
					return false;
				}
			}
		} else {
			afterLast();
			while (newRow++ < 0) {
				if (!previous()) {
					return false;
				}
			}
		}
		return true;
	}

	public boolean relative(int rows) throws Exception {
		if (rows == 0)
			return (row != 0);
		if (rows > 0) {
			while (rows-- > 0) {
				if (!next()) {
					return false;
				}
			}
		} else {
			while (rows++ < 0) {
				if (!previous()) {
					return false;
				}
			}
		}
		return true;
	}

	public int getRow() {
		return row > getRowCount() ? 0 : row;
	}

	public final long getRowPosition() {
		return rowSource.getRowPosition();
	}

	public final void setRowPosition(long rowPosition) throws Exception {
		rowSource.setRowPosition(rowPosition);
		useSetRowPosition = true;
	}

	public final boolean rowInserted() {
		return rowSource.rowInserted();
	}

	public final boolean rowDeleted() {
		return rowSource.rowDeleted();
	}

	public void nullRow() {
		rowSource.nullRow();
		row = 0;

	}

	public void noRow() {
		rowSource.noRow();
		row = 0;
	}

	/**
	 * @inheritDoc
	 */
	public boolean isExpressionsFromThisRowSource(Expressions columns) {
		return rowSource.isExpressionsFromThisRowSource(columns);
	}

	/**
	 * Get the current known row count. This is the sum of queried, sorted rows
	 * and inserted rows.
	 */
	private final int getRowCount() {
		return sortedRowCount + insertedRows.size();
	}

	/**
	 * Calculate the row position in the inserted rows. This is a pointer to
	 * insertedRows. If the row pointer is not in the inserted rows then the
	 * value is negative.
	 */
	private final int currentInsertedRow() {
		return row - sortedRowCount - 1;
	}

}
