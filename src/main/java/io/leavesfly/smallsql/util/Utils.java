/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2011, by Volker Berlin.
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
 * Utils.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.SQLException;

import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.rdb.command.dql.CommandSelect;
import io.leavesfly.smallsql.rdb.engine.Database;
import io.leavesfly.smallsql.rdb.engine.selector.result.MemoryResult;
import io.leavesfly.smallsql.rdb.sql.expression.Expression;
import io.leavesfly.smallsql.rdb.sql.expression.ExpressionName;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;

/**
 * 工具类，提供各种实用方法，包括文件名创建、字符串匹配、数值转换、字节操作、表达式处理等功能。
 */
public class Utils {

	public static final String MASTER_FILENAME = "smallsql.master";
	public static final String TABLE_VIEW_EXTENTION = ".sdb";
	private static final String LOB_EXTENTION = ".lob";
	public static final String IDX_EXTENTION = ".idx";
	private static final Integer[] integerCache = new Integer[260];
	private static final Short[] shortCache = new Short[260];

	static {
		for (int i = -4; i < 256; i++) {
			integerCache[i + 4] = new Integer(i);
			shortCache[i + 4] = new Short((short) i);
		}
	}

	/**
	 * 创建表或视图的文件名
	 * 
	 * @param database 数据库实例
	 * @param name 表或视图名称
	 * @return 完整的文件路径
	 */
	public static String createTableViewFileName(Database database, String name) {
		return database.getName() + '/' + name + TABLE_VIEW_EXTENTION;
	}

	/**
	 * 创建LOB文件名
	 * 
	 * @param database 数据库实例
	 * @param name LOB名称
	 * @return 完整的LOB文件路径
	 */
	public static String createLobFileName(Database database, String name) {
		return database.getName() + '/' + name + LOB_EXTENTION;
	}

	/**
	 * 创建索引文件名
	 * 
	 * @param database 数据库实例
	 * @param name 索引名称
	 * @return 完整的索引文件路径
	 */
	public static String createIdxFileName(Database database, String name) {
		return database.getName() + '/' + name + IDX_EXTENTION;
	}

	/**
	 * 实现SQL LIKE操作符的匹配逻辑
	 * 
	 * @param value 要匹配的字符串值
	 * @param pattern 匹配模式，可包含%和_通配符
	 * @return 如果匹配成功返回true，否则返回false
	 */
	public static boolean like(String value, String pattern) {
		if (value == null || pattern == null)
			return false;
		if (pattern.length() == 0)
			return true;

		int mIdx = 0;// index in mask Array
		int sIdx = 0;// index in search Array
		boolean range = false;
		weiter: while (pattern.length() > mIdx && value.length() > sIdx) {
			char m = Character.toUpperCase(pattern.charAt(mIdx++));
			switch (m) {
			case '%':
				range = true;
				break;
			case '_':
				sIdx++;
				break;
			default:
				if (range) {// % wildcard is active
					for (; sIdx < value.length(); sIdx++) {
						if (Character.toUpperCase(value.charAt(sIdx)) == m)
							break;// Counter mustn't increment before break
					}
					if (sIdx >= value.length())
						return false;
					int lastmIdx = mIdx - 1;
					sIdx++;
					while (pattern.length() > mIdx && value.length() > sIdx) {
						m = Character.toUpperCase(pattern.charAt(mIdx++));
						if (Character.toUpperCase(value.charAt(sIdx)) != m) {
							if (m == '%' || m == '_') {
								mIdx--;
								break;
							}
							mIdx = lastmIdx;
							continue weiter;
						}
						sIdx++;
					}
					range = false;
				} else {
					if (Character.toUpperCase(value.charAt(sIdx)) != m)
						return false;
					sIdx++;
				}
				break;
			}
		}
		while (pattern.length() > mIdx) {
			// Search mask is not too ends yet it may only '%' be contained
			if (Character.toUpperCase(pattern.charAt(mIdx++)) != '%')
				return false;
		}
		while (value.length() > sIdx && !range)
			return false;
		return true;
	}

	/**
	 * 将long值转换为int值，如果超出范围则返回边界值
	 * 
	 * @param value 要转换的long值
	 * @return 转换后的int值
	 */
	public static int long2int(long value) {
		if (value > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;
		if (value < Integer.MIN_VALUE)
			return Integer.MIN_VALUE;
		return (int) value;
	}

	/**
	 * 将double值转换为long值，如果超出范围则返回边界值
	 * 
	 * @param value 要转换的double值
	 * @return 转换后的long值
	 */
	public static long double2long(double value) {
		if (value > Long.MAX_VALUE)
			return Long.MAX_VALUE;
		if (value < Long.MIN_VALUE)
			return Long.MIN_VALUE;
		return (long) value;
	}

	/**
	 * 将字节数组转换为float值
	 * 
	 * @param bytes 包含float值的字节数组
	 * @return 转换后的float值
	 */
	public static float bytes2float(byte[] bytes) {
		return Float.intBitsToFloat(bytes2int(bytes));
	}

	/**
	 * 将字节数组转换为double值
	 * 
	 * @param bytes 包含double值的字节数组
	 * @return 转换后的double值
	 */
	public static double bytes2double(byte[] bytes) {
		return Double.longBitsToDouble(bytes2long(bytes));
	}

	/**
	 * 将字节数组转换为long值
	 * 
	 * @param bytes 包含long值的字节数组
	 * @return 转换后的long值
	 */
	public static long bytes2long(byte[] bytes) {
		long result = 0;
		int length = Math.min(8, bytes.length);
		for (int i = 0; i < length; i++) {
			result = (result << 8) | (bytes[i] & 0xFF);
		}
		return result;
	}

	/**
	 * 将字节数组转换为int值
	 * 
	 * @param bytes 包含int值的字节数组
	 * @return 转换后的int值
	 */
	public static int bytes2int(byte[] bytes) {
		int result = 0;
		int length = Math.min(4, bytes.length);
		for (int i = 0; i < length; i++) {
			result = (result << 8) | (bytes[i] & 0xFF);
		}
		return result;
	}

	/**
	 * 将double值转换为字节数组
	 * 
	 * @param value 要转换的double值
	 * @return 包含double值的字节数组
	 */
	public static byte[] double2bytes(double value) {
		return long2bytes(Double.doubleToLongBits(value));
	}

	/**
	 * 将float值转换为字节数组
	 * 
	 * @param value 要转换的float值
	 * @return 包含float值的字节数组
	 */
	public static byte[] float2bytes(float value) {
		return int2bytes(Float.floatToIntBits(value));
	}

	/**
	 * 将long值转换为字节数组
	 * 
	 * @param value 要转换的long值
	 * @return 包含long值的字节数组
	 */
	public static byte[] long2bytes(long value) {
		byte[] result = new byte[8];
		result[0] = (byte) (value >> 56);
		result[1] = (byte) (value >> 48);
		result[2] = (byte) (value >> 40);
		result[3] = (byte) (value >> 32);
		result[4] = (byte) (value >> 24);
		result[5] = (byte) (value >> 16);
		result[6] = (byte) (value >> 8);
		result[7] = (byte) (value);
		return result;
	}

	/**
	 * 将long值转换为int值用于货币计算，如果超出范围则返回边界值
	 * 
	 * @param value 要转换的long值
	 * @return 转换后的int值
	 */
	public static int money2int(long value) {
		if (value < Integer.MIN_VALUE)
			return Integer.MIN_VALUE;
		else if (value > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;
		else
			return (int) value;
	}

	/**
	 * 将int值转换为字节数组
	 * 
	 * @param value 要转换的int值
	 * @return 包含int值的字节数组
	 */
	public static byte[] int2bytes(int value) {
		byte[] result = new byte[4];
		result[0] = (byte) (value >> 24);
		result[1] = (byte) (value >> 16);
		result[2] = (byte) (value >> 8);
		result[3] = (byte) (value);
		return result;
	}

	/**
	 * 将字节数组转换为十六进制字符串
	 * 
	 * @param bytes 要转换的字节数组
	 * @return 十六进制字符串表示
	 */
	public static String bytes2hex(byte[] bytes) {
		StringBuffer buf = new StringBuffer(bytes.length << 1);
		for (int i = 0; i < bytes.length; i++) {
			buf.append(digits[(bytes[i] >> 4) & 0x0F]);
			buf.append(digits[(bytes[i]) & 0x0F]);
		}
		return buf.toString();
	}

	/**
	 * 将十六进制字符数组转换为字节数组
	 * 
	 * @param hex 十六进制字符数组
	 * @param offset 起始偏移量
	 * @param length 要转换的长度
	 * @return 转换后的字节数组
	 * @throws SQLException 当十六进制字符串无效时抛出
	 */
	public static byte[] hex2bytes(char[] hex, int offset, int length) throws SQLException {
		try {
			byte[] bytes = new byte[length / 2];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = (byte) ((hexDigit2int(hex[offset++]) << 4) | hexDigit2int(hex[offset++]));
			}
			return bytes;
		} catch (Exception e) {
			throw SmallSQLException.create(Language.SEQUENCE_HEX_INVALID, String.valueOf(offset)); /*
																									 * ,
																									 * offset
																									 */
		}
	}

	/**
	 * 将十六进制字符转换为整数值
	 * 
	 * @param digit 十六进制字符('0'-'9', 'a'-'f', 'A'-'F')
	 * @return 对应的整数值(0-15)
	 */
	private static int hexDigit2int(char digit) {
		if (digit >= '0' && digit <= '9')
			return digit - '0';
		digit |= 0x20; // Convert to lowercase
		if (digit >= 'a' && digit <= 'f')
			return digit - 'a' + 10; // Convert hex letter to number
		throw new RuntimeException();
	}

	/**
	 * 将UUID字符串转换为字节数组
	 * 
	 * @param unique UUID字符串
	 * @return 对应的16字节数据
	 * @throws SQLException 当UUID字符串格式无效时抛出
	 */
	public static byte[] unique2bytes(String unique) throws SQLException {
		char[] chars = unique.toCharArray();
		byte[] daten = new byte[16];
		daten[3] = hex2byte(chars, 0);
		daten[2] = hex2byte(chars, 2);
		daten[1] = hex2byte(chars, 4);
		daten[0] = hex2byte(chars, 6);

		daten[5] = hex2byte(chars, 9);
		daten[4] = hex2byte(chars, 11);

		daten[7] = hex2byte(chars, 14);
		daten[6] = hex2byte(chars, 16);

		daten[8] = hex2byte(chars, 19);
		daten[9] = hex2byte(chars, 21);

		daten[10] = hex2byte(chars, 24);
		daten[11] = hex2byte(chars, 26);
		daten[12] = hex2byte(chars, 28);
		daten[13] = hex2byte(chars, 30);
		daten[14] = hex2byte(chars, 32);
		daten[15] = hex2byte(chars, 34);
		return daten;
	}

	/**
	 * 将十六进制字符数组中的两个字符转换为一个字节
	 * 
	 * @param hex 十六进制字符数组
	 * @param offset 起始偏移量
	 * @return 转换后的字节值
	 * @throws SQLException 当字符不是有效的十六进制字符时抛出
	 */
	private static byte hex2byte(char[] hex, int offset) throws SQLException {
		try {
			return (byte) ((hexDigit2int(hex[offset++]) << 4) | hexDigit2int(hex[offset++]));
		} catch (Exception e) {
			throw SmallSQLException.create(Language.SEQUENCE_HEX_INVALID_STR, new Object[] { new Integer(offset),
					new String(hex) });
		}
	}

	/**
	 * 将字节数组转换为UUID字符串
	 * 
	 * @param daten 包含UUID数据的字节数组
	 * @param offset 起始偏移量
	 * @return UUID字符串表示
	 */
	public static String bytes2unique(byte[] daten, int offset) {
		if (daten.length - offset < 16) {
			byte[] temp = new byte[16];
			System.arraycopy(daten, offset, temp, 0, daten.length - offset);
			daten = temp;
		}
		char[] chars = new char[36];
		chars[8] = chars[13] = chars[18] = chars[23] = '-';

		chars[0] = digits[(daten[offset + 3] >> 4) & 0x0F];
		chars[1] = digits[(daten[offset + 3]) & 0x0F];
		chars[2] = digits[(daten[offset + 2] >> 4) & 0x0F];
		chars[3] = digits[(daten[offset + 2]) & 0x0F];
		chars[4] = digits[(daten[offset + 1] >> 4) & 0x0F];
		chars[5] = digits[(daten[offset + 1]) & 0x0F];
		chars[6] = digits[(daten[offset + 0] >> 4) & 0x0F];
		chars[7] = digits[(daten[offset + 0]) & 0x0F];

		chars[9] = digits[(daten[offset + 5] >> 4) & 0x0F];
		chars[10] = digits[(daten[offset + 5]) & 0x0F];
		chars[11] = digits[(daten[offset + 4] >> 4) & 0x0F];
		chars[12] = digits[(daten[offset + 4]) & 0x0F];

		chars[14] = digits[(daten[offset + 7] >> 4) & 0x0F];
		chars[15] = digits[(daten[offset + 7]) & 0x0F];
		chars[16] = digits[(daten[offset + 6] >> 4) & 0x0F];
		chars[17] = digits[(daten[offset + 6]) & 0x0F];

		chars[19] = digits[(daten[offset + 8] >> 4) & 0x0F];
		chars[20] = digits[(daten[offset + 8]) & 0x0F];
		chars[21] = digits[(daten[offset + 9] >> 4) & 0x0F];
		chars[22] = digits[(daten[offset + 9]) & 0x0F];

		chars[24] = digits[(daten[offset + 10] >> 4) & 0x0F];
		chars[25] = digits[(daten[offset + 10]) & 0x0F];
		chars[26] = digits[(daten[offset + 11] >> 4) & 0x0F];
		chars[27] = digits[(daten[offset + 11]) & 0x0F];
		chars[28] = digits[(daten[offset + 12] >> 4) & 0x0F];
		chars[29] = digits[(daten[offset + 12]) & 0x0F];
		chars[30] = digits[(daten[offset + 13] >> 4) & 0x0F];
		chars[31] = digits[(daten[offset + 13]) & 0x0F];
		chars[32] = digits[(daten[offset + 14] >> 4) & 0x0F];
		chars[33] = digits[(daten[offset + 14]) & 0x0F];
		chars[34] = digits[(daten[offset + 15] >> 4) & 0x0F];
		chars[35] = digits[(daten[offset + 15]) & 0x0F];
		return new String(chars);
	}

	/**
	 * 将字符串转换为布尔值
	 * 
	 * @param val 要转换的字符串
	 * @return 如果字符串表示true则返回true，否则返回false
	 */
	public static boolean string2boolean(String val) {
		try {
			return Double.parseDouble(val) != 0;
		} catch (NumberFormatException e) {/* ignore it if it not a number */
		}
		return "true".equalsIgnoreCase(val) || "yes".equalsIgnoreCase(val) || "t".equalsIgnoreCase(val);
	}

	/**
	 * 将double值转换为货币值(long类型，精确到万分之一)
	 * 
	 * @param value 要转换的double值
	 * @return 转换后的货币值
	 */
	public static long doubleToMoney(double value) {
		if (value < 0)
			return (long) (value * 10000 - 0.5);
		return (long) (value * 10000 + 0.5);
	}

	/**
	 * 在字符数组中查找指定字符（大小写不敏感）
	 * 
	 * @param value 要查找的字符
	 * @param str 字符数组
	 * @param offset 起始偏移量
	 * @param length 查找长度
	 * @return 找到的字符索引，未找到返回-1
	 */
	public static int indexOf(char value, char[] str, int offset, int length) {
		value |= 0x20; // Convert to lowercase
		for (int end = offset + length; offset < end; offset++) {
			if ((str[offset] | 0x20) == value) // Compare case-insensitive
				return offset;
		}
		return -1;
	}

	/**
	 * 在整数数组中查找指定值
	 * 
	 * @param value 要查找的值
	 * @param list 整数数组
	 * @return 找到的值索引，未找到返回-1
	 */
	public static int indexOf(int value, int[] list) {
		int offset = 0;
		for (int end = list.length; offset < end; offset++) {
			if ((list[offset]) == value)
				return offset;
		}
		return -1;
	}

	/**
	 * 在字节数组中查找指定的字节序列
	 * 
	 * @param value 要查找的字节序列
	 * @param list 被查找的字节数组
	 * @param offset 起始偏移量
	 * @return 找到的字节序列起始索引，未找到返回-1
	 */
	public static int indexOf(byte[] value, byte[] list, int offset) {
		int length = value.length;
		loop1: for (int end = list.length - length; offset <= end; offset++) {
			for (int i = 0; i < length; i++) {
				if (list[offset + i] != value[i]) {
					continue loop1;
				}
			}
			return offset;
		}
		return -1;
	}

	/**
	 * 比较两个字节数组
	 * 
	 * @param leftBytes 左侧字节数组
	 * @param rightBytes 右侧字节数组
	 * @return 比较结果：相等返回0，小于返回-1，大于返回1
	 */
	public static int compareBytes(byte[] leftBytes, byte[] rightBytes) {
		int length = Math.min(leftBytes.length, rightBytes.length);
		int comp = 0;
		for (int i = 0; i < length; i++) {
			if (leftBytes[i] != rightBytes[i]) {
				comp = leftBytes[i] < rightBytes[i] ? -1 : 1;
				break;
			}
		}
		if (comp == 0 && leftBytes.length != rightBytes.length) {
			comp = leftBytes.length < rightBytes.length ? -1 : 1;
		}
		return comp;
	}

	/**
	 * 
	 * @param colNames
	 * @param data
	 * @return
	 * @throws SQLException
	 */
	public static CommandSelect createMemoryCommandSelect(SsConnection con, String[] colNames, Object[][] data)
			throws SQLException {
		MemoryResult source = new MemoryResult(data, colNames.length);
		CommandSelect cmd = new CommandSelect(con.log);
		for (int i = 0; i < colNames.length; i++) {
			ExpressionName expr = new ExpressionName(colNames[i]);
			cmd.addColumnExpression(expr);
			expr.setFrom(source, i, source.getColumn(i));
		}
		cmd.setSource(source);
		return cmd;
	}

	/**
	 * recycle Integer objects, this is faster as to garbage the objects
	 */
	public static final Integer getInteger(int value) {
		if (value >= -4 && value < 256) {
			return integerCache[value + 4];
		} else
			return new Integer(value);
	}

	/**
	 * recycle Integer objects, this is faster as to garbage the objects
	 */
	public static final Short getShort(int value) {
		if (value >= -4 && value < 256) {
			return shortCache[value + 4];
		} else
			return new Short((short) value);
	}

	/**
	 * Open a RandomAccessFile and lock it that no other thread or VM can open
	 * it..
	 * 
	 * @param file
	 *            The file that should be open.
	 * @return a FileChannel
	 * @throws FileNotFoundException
	 *             If the file can not open
	 * @param readonly
	 *            open database in read only mode
	 * @throws SQLException
	 *             If the file can't lock.
	 */
	public static final FileChannel openRaFile(File file, boolean readonly) throws FileNotFoundException, SQLException {
		RandomAccessFile raFile = new RandomAccessFile(file, readonly ? "r" : "rw");
		FileChannel channel = raFile.getChannel();
		if (!readonly) {
			try {
				FileLock lock = channel.tryLock();
				if (lock == null) {
					throw SmallSQLException.create(Language.CANT_LOCK_FILE, file);
				}
			} catch (SQLException sqlex) {
				try {
					raFile.close();
				} catch (Exception e) { /* ignore */ }
				throw sqlex;
			} catch (Throwable th) {
				try {
					raFile.close();
				} catch (Exception e) { /* ignore */ }
				throw SmallSQLException.createFromException(Language.CANT_LOCK_FILE, file, th);
			}
		}
		return channel;
	}

	/**
	 * Get all the ExpressionName objects that are part of the tree. If it only
	 * a constant expression then a empty list is return.
	 * 
	 * @param tree
	 *            the expression to scan
	 * @return the list of ExpressionName instances
	 */
	public static final Expressions getExpressionNameFromTree(Expression tree) {
		Expressions list = new Expressions();
		getExpressionNameFromTree(list, tree);
		return list;
	}

	/**
	 * Scan the tree recursively.
	 */
	private static final void getExpressionNameFromTree(Expressions list, Expression tree) {
		if (tree.getType() == Expression.NAME) {
			list.add(tree);
		}
		Expression[] params = tree.getParams();
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				getExpressionNameFromTree(list, params[i]);
			}
		}
	}

	public final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
			'F' };
}