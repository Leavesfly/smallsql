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
 * Join.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.engine.selector.multioper;

import io.leavesfly.smallsql.rdb.engine.Index;
import io.leavesfly.smallsql.rdb.engine.RowSource;
import io.leavesfly.smallsql.rdb.sql.expression.Expression;
import io.leavesfly.smallsql.util.datastruct.LongTreeListEnum;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;
import io.leavesfly.smallsql.rdb.sql.expression.operator.ExpressionArithmetic;
import io.leavesfly.smallsql.util.Utils;
import io.leavesfly.smallsql.util.datastruct.LongLongList;
import io.leavesfly.smallsql.util.datastruct.LongTreeList;

public final class Join extends RowSource {

	public Expression condition; // the join condition, the part after the ON
	private int type;
	public RowSource left; // the left table, view or rowsource of the join
	public RowSource right;
	private boolean isAfterLast;

	private LongLongList rowPositions; // needed for getRowPosition() and
										// setRowPosition()
	private int row; // current row number

	JoinScroll scroll;

	public Join(int type, RowSource left, RowSource right, Expression condition) {
		this.type = type;
		this.condition = condition;
		this.left = left;
		this.right = right;
	}

	public final boolean isScrollable() {
		return false; // TODO performance, if left and right are scrollable then
						// this should also scrollable
	}

	public void beforeFirst() throws Exception {
		scroll.beforeFirst();
		isAfterLast = false;
		row = 0;
	}

	public boolean first() throws Exception {
		beforeFirst();
		return next();
	}

	public boolean next() throws Exception {
		if (isAfterLast)
			return false;
		row++;
		boolean result = scroll.next();
		if (!result) {
			noRow();
		}
		return result;
	}

	public void afterLast() {
		isAfterLast = true;
		noRow();
	}

	public int getRow() {
		return row;
	}

	public final long getRowPosition() {
		if (rowPositions == null)
			rowPositions = new LongLongList();
		rowPositions.add(left.getRowPosition(), right.getRowPosition());
		return rowPositions.size() - 1;
	}

	public final void setRowPosition(long rowPosition) throws Exception {
		left.setRowPosition(rowPositions.get1((int) rowPosition));
		right.setRowPosition(rowPositions.get2((int) rowPosition));
	}

	public final boolean rowInserted() {
		return left.rowInserted() || right.rowInserted();
	}

	public final boolean rowDeleted() {
		return left.rowDeleted() || right.rowDeleted();
	}

	/**
	 * By OUTER or FULL JOIN must one rowsource set to null.
	 */
	public void nullRow() {
		left.nullRow();
		right.nullRow();
		row = 0;
	}

	public void noRow() {
		isAfterLast = true;
		left.noRow();
		right.noRow();
		row = 0;
	}

	public void execute() throws Exception {
		left.execute();
		right.execute();
		// create the best join algorithm
		if (!createJoinScrollIndex()) {
			// Use the default join algorithm with a loop as fallback
			scroll = new JoinScroll(type, left, right, condition);
		}
	}

	/**
	 * @inheritDoc
	 */
	public boolean isExpressionsFromThisRowSource(Expressions columns) {
		if (left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)) {
			return true;
		}
		if (columns.size() == 1) {
			return false;
		}

		// Now it will be difficult, there are 2 or more column
		// one can in the left, the other can be in the right
		// Or one is not in both that we need to check everyone individually
		Expressions single = new Expressions();
		for (int i = 0; i < columns.size(); i++) {
			single.clear();
			single.add(columns.get(i));
			if (left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)) {
				continue;
			}
			return false;
		}
		return true;
	}

	/**
	 * Create a ScrollJoin that based on a index. There must not exist a index
	 * on a table. If there is no index then a index will be created.
	 * 
	 * @return null if it is not possible to create a ScrollJoin based on a
	 *         Index
	 */
	private boolean createJoinScrollIndex() throws Exception {
		if (type == CROSS_JOIN) {
			return false;
		}
		if (type != INNER_JOIN) {
			// TODO currently only INNER JOIN are implemented
			return false;
		}
		if (condition instanceof ExpressionArithmetic) {
			ExpressionArithmetic cond = (ExpressionArithmetic) condition;
			Expressions leftEx = new Expressions();
			Expressions rightEx = new Expressions();
			int operation = createJoinScrollIndex(cond, leftEx, rightEx, 0);
			if (operation != 0) {
				scroll = new JoinScrollIndex(type, left, right, leftEx, rightEx, operation);
				return true;
			}
		}
		return false;
	}

	private static class JoinScrollIndex extends JoinScroll {

		private final int compare;

		Expressions leftEx;
		Expressions rightEx;

		private Index index;

		private LongTreeList rowList;

		private final LongTreeListEnum longListEnum = new LongTreeListEnum();

		JoinScrollIndex(int joinType, RowSource left, RowSource right, Expressions leftEx, Expressions rightEx,
				int compare) throws Exception {
			super(joinType, left, right, null);
			this.leftEx = leftEx;
			this.rightEx = rightEx;
			this.compare = compare;
			createIndex(rightEx);
		}

		private void createIndex(Expressions rightEx) throws Exception {
			index = new Index(false);
			right.beforeFirst();
			while (right.next()) {
				index.addValues(right.getRowPosition(), rightEx);
			}
		}

		boolean next() throws Exception {
			switch (compare) {
			case ExpressionArithmetic.EQUALS:
				return nextEquals();
			default:
				throw new Error("Compare operation not supported:" + compare);
			}

		}

		private boolean nextEquals() throws Exception {
			if (rowList != null) {
				long rowPosition = rowList.getNext(longListEnum);
				if (rowPosition != -1) {
					right.setRowPosition(rowPosition);
					return true;
				}
				rowList = null;
			}
			Object rows;
			do {
				if (!left.next()) {
					return false;
				}
				rows = index.findRows(leftEx, false, null);
			} while (rows == null);

			if (rows instanceof Long) {
				right.setRowPosition(((Long) rows).longValue());
			} else {
				rowList = (LongTreeList) rows;
				longListEnum.reset();
				right.setRowPosition(rowList.getNext(longListEnum));
			}
			return true;
		}

	}

	private int createJoinScrollIndex(ExpressionArithmetic cond, Expressions leftEx, Expressions rightEx, int operation)
			throws Exception {
		Expression[] params = cond.getParams();
		int op = cond.getOperation();
		if (op == ExpressionArithmetic.AND) {
			Expression param0 = params[0];
			Expression param1 = params[1];
			if (param0 instanceof ExpressionArithmetic && param1 instanceof ExpressionArithmetic) {
				op = createJoinScrollIndex((ExpressionArithmetic) param0, leftEx, rightEx, operation);
				if (op == 0) {
					return 0;
				}
				return createJoinScrollIndex((ExpressionArithmetic) param1, leftEx, rightEx, operation);
			}
			return 0;
		}
		if (operation == 0) {
			operation = op;
		}
		if (operation != op) {
			return 0;
		}
		if (operation == ExpressionArithmetic.EQUALS) {
			Expression param0 = params[0];
			Expression param1 = params[1];
			// scan all column that are include in the expression
			Expressions columns0 = Utils.getExpressionNameFromTree(param0);
			Expressions columns1 = Utils.getExpressionNameFromTree(param1);
			if (left.isExpressionsFromThisRowSource(columns0) && right.isExpressionsFromThisRowSource(columns1)) {
				leftEx.add(param0);
				rightEx.add(param1);
			} else {
				if (left.isExpressionsFromThisRowSource(columns1) && right.isExpressionsFromThisRowSource(columns0)) {
					leftEx.add(param1);
					rightEx.add(param0);
				} else {
					return 0;
				}
			}

			return operation;
		}
		return 0;
	}

	public static final int CROSS_JOIN = 1;
	public static final int INNER_JOIN = 2;
	public static final int LEFT_JOIN = 3;
	public static final int FULL_JOIN = 4;
	public static final int RIGHT_JOIN = 5;

	private static class JoinScroll {

		// the join condition, the part
		// after
		// the ON
		private final Expression condition;
		final int type;
		final RowSource left; // the left table, view or rowsource of the join
		final RowSource right;

		private boolean isBeforeFirst = true;
		private boolean isOuterValid = true;

		// Variables for FULL JOIN
		private boolean[] isFullNotValid;
		private int fullRightRowCounter;
		private int fullRowCount;
		private int fullReturnCounter = -1;

		JoinScroll(int type, RowSource left, RowSource right, Expression condition) {
			this.type = type;
			this.condition = condition;
			this.left = left;
			this.right = right;
			if (type == Join.FULL_JOIN) {
				isFullNotValid = new boolean[10];
			}
		}

		void beforeFirst() throws Exception {
			left.beforeFirst();
			right.beforeFirst();
			isBeforeFirst = true;
			fullRightRowCounter = 0;
			fullRowCount = 0;
			fullReturnCounter = -1;
		}

		boolean next() throws Exception {
			boolean result;
			if (fullReturnCounter >= 0) {
				do {
					if (fullReturnCounter >= fullRowCount) {
						return false;
					}
					right.next();
				} while (isFullNotValid[fullReturnCounter++]);
				return true;
			}
			do {
				if (isBeforeFirst) {
					result = left.next();
					if (result) {
						result = right.first();
						if (!result) {
							switch (type) {
							case Join.LEFT_JOIN:
							case Join.FULL_JOIN:
								isOuterValid = false;
								isBeforeFirst = false;
								right.nullRow();
								return true;
							}
						} else
							fullRightRowCounter++;
					} else {
						// left does not include any row
						if (type == Join.FULL_JOIN) {
							while (right.next()) {
								fullRightRowCounter++;
							}
							fullRowCount = fullRightRowCounter;
						}
					}
				} else {
					result = right.next();
					if (!result) {
						switch (type) {
						case Join.LEFT_JOIN:
						case Join.FULL_JOIN:
							if (isOuterValid) {
								isOuterValid = false;
								right.nullRow();
								return true;
							}
							fullRowCount = Math.max(fullRowCount, fullRightRowCounter);
							fullRightRowCounter = 0;
						}
						isOuterValid = true;
						result = left.next();
						if (result) {
							result = right.first();
							if (!result) {
								switch (type) {
								case Join.LEFT_JOIN:
								case Join.FULL_JOIN:
									isOuterValid = false;
									right.nullRow();
									return true;
								}
							} else
								fullRightRowCounter++;
						}

					} else
						fullRightRowCounter++;
				}
				isBeforeFirst = false;
			} while (result && !getBoolean());
			isOuterValid = false;
			if (type == Join.FULL_JOIN) {
				if (fullRightRowCounter >= isFullNotValid.length) {
					boolean[] temp = new boolean[fullRightRowCounter << 1];
					System.arraycopy(isFullNotValid, 0, temp, 0, fullRightRowCounter);
					isFullNotValid = temp;
				}
				if (!result) {
					if (fullRowCount == 0) {
						return false;
					}
					if (fullReturnCounter < 0) {
						fullReturnCounter = 0;
						right.first();
						left.nullRow();
					}
					while (isFullNotValid[fullReturnCounter++]) {
						if (fullReturnCounter >= fullRowCount) {
							return false;
						}
						right.next();
					}
					return true;
				} else
					isFullNotValid[fullRightRowCounter - 1] = result;
			}
			return result;
		}

		private boolean getBoolean() throws Exception {
			return type == Join.CROSS_JOIN || condition.getBoolean();
		}
	}
}