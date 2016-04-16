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
 * Created on 31.07.2004
 */
package io.leavesfly.smallsql.rdb.engine.selector.multioper;

import io.leavesfly.smallsql.rdb.engine.Index;
import io.leavesfly.smallsql.rdb.engine.RowSource;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;

/**
 * @author Volker Berlin
 */
public final class Distinct extends RowSource {

	final private Expressions distinctColumns;
	final private RowSource rowSource;
	private Index index;
	private int row;

	public Distinct(RowSource rowSource, Expressions columns) {
		this.rowSource = rowSource;
		this.distinctColumns = columns;
	}

	public final void execute() throws Exception {
		rowSource.execute();
		index = new Index(true);
	}

	public final boolean isScrollable() {
		return false;
	}

	public final void beforeFirst() throws Exception {
		rowSource.beforeFirst();
		row = 0;
	}

	public final boolean first() throws Exception {
		beforeFirst();
		return next();
	}

	public final boolean next() throws Exception {
		while (true) {
			boolean isNext = rowSource.next();
			if (!isNext)
				return false;

			Long oldRowOffset = (Long) index.findRows(distinctColumns, true, null);
			long newRowOffset = rowSource.getRowPosition();
			if (oldRowOffset == null) {
				index.addValues(newRowOffset, distinctColumns);
				row++;
				return true;
			} else if (oldRowOffset.longValue() == newRowOffset) {
				row++;
				return true;
			}
		}
	}

	public final void afterLast() throws Exception {
		rowSource.afterLast();
		row = 0;
	}

	public final int getRow() throws Exception {
		return row;
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

	/**
	 * @inheritDoc
	 */
	public boolean isExpressionsFromThisRowSource(Expressions columns) {
		return rowSource.isExpressionsFromThisRowSource(columns);
	}
}
