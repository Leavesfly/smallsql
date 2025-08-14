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
 * SQLParser.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.sql;

import java.util.List;
import java.sql.*;

import io.leavesfly.smallsql.rdb.command.ddl.CommandCreateDatabase;
import io.leavesfly.smallsql.rdb.command.ddl.CommandSet;
import io.leavesfly.smallsql.rdb.command.dml.CommandInsert;
import io.leavesfly.smallsql.rdb.command.dml.CommandUpdate;
import io.leavesfly.smallsql.rdb.engine.RowSource;
import io.leavesfly.smallsql.rdb.engine.table.Column;
import io.leavesfly.smallsql.rdb.engine.table.ForeignKey;
import io.leavesfly.smallsql.rdb.sql.datatype.Strings;
import io.leavesfly.smallsql.rdb.sql.expression.Expression;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionDayOfYear;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionMinute;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionYear;
import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.command.Command;
import io.leavesfly.smallsql.rdb.command.ddl.CommandCreateView;
import io.leavesfly.smallsql.rdb.command.ddl.CommandDrop;
import io.leavesfly.smallsql.rdb.command.ddl.CommandTable;
import io.leavesfly.smallsql.rdb.command.dml.CommandDelete;
import io.leavesfly.smallsql.rdb.command.dql.CommandSelect;
import io.leavesfly.smallsql.rdb.engine.Database;
import io.leavesfly.smallsql.rdb.engine.View;
import io.leavesfly.smallsql.rdb.engine.index.IndexDescription;
import io.leavesfly.smallsql.rdb.engine.selector.DataSources;
import io.leavesfly.smallsql.rdb.engine.selector.multioper.Join;
import io.leavesfly.smallsql.rdb.engine.selector.multioper.UnionAll;
import io.leavesfly.smallsql.rdb.engine.selector.result.TableViewResult;
import io.leavesfly.smallsql.rdb.engine.selector.result.ViewResult;
import io.leavesfly.smallsql.rdb.sql.datatype.DateTime;
import io.leavesfly.smallsql.rdb.sql.datatype.Money;
import io.leavesfly.smallsql.rdb.sql.expression.ExpressionName;
import io.leavesfly.smallsql.rdb.sql.expression.ExpressionValue;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;
import io.leavesfly.smallsql.rdb.sql.expression.function.ExpressionFunctionReplace;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionACos;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionASin;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionATan;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionATan2;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionAbs;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionCeiling;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionCos;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionCot;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionDegrees;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionExp;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionFloor;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionLog;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionLog10;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionMod;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionPI;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionPower;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionRadians;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionRand;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionRound;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionSign;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionSin;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionSqrt;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionTan;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.ExpressionFunctionTruncate;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionAscii;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionBitLen;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionChar;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionCharLen;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionDifference;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionInsert;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionLCase;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionLTrim;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionLeft;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionLength;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionLocate;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionOctetLen;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionRTrim;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionRepeat;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionRight;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionSoundex;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionSpace;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionSubstring;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.ExpressionFunctionUCase;
import io.leavesfly.smallsql.rdb.sql.expression.function.system.ExpressionFunctionCase;
import io.leavesfly.smallsql.rdb.sql.expression.function.system.ExpressionFunctionConvert;
import io.leavesfly.smallsql.rdb.sql.expression.function.system.ExpressionFunctionIIF;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionDayOfMonth;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionDayOfWeek;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionHour;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionMonth;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionTimestampAdd;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.ExpressionFunctionTimestampDiff;
import io.leavesfly.smallsql.rdb.sql.expression.operator.ExpressionArithmetic;
import io.leavesfly.smallsql.rdb.sql.expression.operator.ExpressionInSelect;
import io.leavesfly.smallsql.rdb.sql.parser.SQLToken;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;
import io.leavesfly.smallsql.util.Utils;

/**
 * SQL语句解析器类
 * <p>
 * 该类负责将SQL语句字符串解析为可执行的命令对象。它能够处理各种SQL语句，
 * 包括SELECT、INSERT、UPDATE、DELETE、CREATE、DROP等操作。
 * </p>
 */
public final class SQLParser {

    /**
     * 数据库连接对象
     */
    SsConnection con;
    
    /**
     * SQL语句字符数组
     */
    protected char[] sql;
    
    /**
     * SQL语句的标记列表
     */
    protected List<SQLToken> tokens;
    
    /**
     * 当前处理的标记索引
     */
    protected int tokenIdx;

    /**
     * 解析SQL语句并返回对应的命令对象
     * 
     * @param con 数据库连接
     * @param sqlString SQL语句字符串
     * @return 解析后的命令对象
     * @throws SQLException SQL异常
     */
    public Command parse(SsConnection con, String sqlString)
            throws SQLException {
        this.con = con;
        Command cmd = parse(sqlString.toCharArray());
        SQLToken token = nextToken();
        if (token != null) {
            throw createSyntaxError(token, Language.STXADD_ADDITIONAL_TOK);
        }
        return cmd;
    }

    /**
     * 解析SQL语句字符数组并返回对应的命令对象
     * 
     * @param sql SQL语句字符数组
     * @return 解析后的命令对象
     * @throws SQLException SQL异常
     */
    final private Command parse(char[] sql) throws SQLException {
        this.sql = sql;
        this.tokens = SQLTokenizer.parseSQL(sql);
        tokenIdx = 0;

        SQLToken token = nextToken(COMMANDS);
        switch (token.value) {
            case SQLTokenizer.SELECT:
                return select();
            case SQLTokenizer.DELETE:
                return delete();
            case SQLTokenizer.INSERT:
                return insert();
            case SQLTokenizer.UPDATE:
                return update();
            case SQLTokenizer.CREATE:
                return create();
            case SQLTokenizer.DROP:
                return drop();
            case SQLTokenizer.ALTER:
                return alter();
            case SQLTokenizer.SET:
                return set();
            case SQLTokenizer.USE:
                token = nextToken(MISSING_EXPRESSION);
                String name = token.getName(sql);
                checkValidIdentifier(name, token);
                CommandSet set = new CommandSet(con.log, SQLTokenizer.USE);
                set.name = name;
                return set;
            case SQLTokenizer.EXECUTE:
                return execute();
            case SQLTokenizer.TRUNCATE:
                return truncate();
            default:
                throw new Error();
        }
    }

    /**
     * 解析表达式字符串
     * 
     * @param expr 表达式字符串
     * @return 解析后的表达式对象
     * @throws SQLException SQL异常
     */
    public Expression parseExpression(String expr) throws SQLException {
        this.sql = expr.toCharArray();
        this.tokens = SQLTokenizer.parseSQL(sql);
        tokenIdx = 0;
        return expression(null, 0);
    }

    /**
     * 创建语法错误消息，使用自定义消息
     *
     * @param token token对象；如果不为null，则生成SYNTAX_BASE_OFS，否则生成SYNTAX_BASE_END
     * @param addMessageCode 附加消息代码
     */
    protected SQLException createSyntaxError(SQLToken token, String addMessageCode) {
        String message = getErrorString(token, addMessageCode, null);
        return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
    }

    /**
     * 创建语法错误消息，使用带参数的消息
     *
     * @param token          token对象；如果不为null，则生成SYNTAX_BASE_OFS，否则生成SYNTAX_BASE_END
     * @param addMessageCode 附加消息代码
     * @param param0         参数
     */
    protected SQLException createSyntaxError(SQLToken token,
                                           String addMessageCode, Object param0) {
        String message = getErrorString(token, addMessageCode, param0);
        return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
    }

    /**
     * 创建"需要附加关键字"的语法错误
     *
     * @param token       token对象
     * @param validValues 有效值数组
     * @return 异常对象
     */
    protected SQLException createSyntaxError(SQLToken token, int[] validValues) {
        String msgStr = SmallSQLException.translateMsg(
                Language.STXADD_KEYS_REQUIRED, new Object[]{});

        StringBuffer msgBuf = new StringBuffer(msgStr);

        for (int i = 0; i < validValues.length; i++) {
            String name = SQLTokenizer.getKeyWord(validValues[i]);
            if (name == null)
                name = String.valueOf((char) validValues[i]);
            msgBuf.append(name);
            if (i < validValues.length - 2)
                msgBuf.append(", ");
            else if (i == validValues.length - 2)
                msgBuf.append(" or ");
        }

        String message = getErrorString(token, Language.CUSTOM_MESSAGE, msgBuf);
        return SmallSQLException.create(Language.CUSTOM_MESSAGE, message);
    }

    /**
     * 创建完整的错误字符串（开始+中间+结束）
     *
     * @param token          token对象
     * @param middleMsgCode  中间消息代码
     * @param middleMsgParam 中间消息参数
     * @return 完整的错误消息字符串
     */
    private String getErrorString(SQLToken token, String middleMsgCode,
                                  Object middleMsgParam) {
        StringBuffer buffer = new StringBuffer(1024);

		/* begin */

        if (token != null) {
            Object[] params = {String.valueOf(token.offset),
                    String.valueOf(sql, token.offset, token.length)};
            String begin = SmallSQLException.translateMsg(
                    Language.SYNTAX_BASE_OFS, params);
            buffer.append(begin);
        } else {
            String begin = SmallSQLException.translateMsg(
                    Language.SYNTAX_BASE_END, new Object[]{});
            buffer.append(begin);
        }

		/* middle */

        String middle = SmallSQLException.translateMsg(middleMsgCode,
                new Object[]{middleMsgParam});

        buffer.append(middle);

		/* end */

        int valOffset = (token != null) ? token.offset : sql.length;
        int valBegin = Math.max(0, valOffset - 40);
        int valEnd = Math.min(valOffset + 20, sql.length);
        String lineSeparator = System.getProperty("line.separator");
        buffer.append(lineSeparator);
        buffer.append(sql, valBegin, valEnd - valBegin);
        buffer.append(lineSeparator);
        for (; valBegin < valOffset; valBegin++)
            buffer.append(' ');
        buffer.append('^');

        return buffer.toString();
    }

    /**
     * 检查标识符是否有效
     * 
     * @param name 标识符名称
     * @param token SQL标记
     * @throws SQLException SQL异常
     */
    protected void checkValidIdentifier(String name, SQLToken token)
            throws SQLException {
        if (token.value == SQLTokenizer.ASTERISK)
            return;
        if (token.value != SQLTokenizer.VALUE
                && token.value != SQLTokenizer.IDENTIFIER && token.value < 200) {
            throw createSyntaxError(token, Language.STXADD_IDENT_EXPECT);
        }
        if (name.length() == 0) {
            throw createSyntaxError(token, Language.STXADD_IDENT_EMPTY, name);
        }
        char firstChar = name.charAt(0);
        if (firstChar != '#' && firstChar < '@') {
            throw createSyntaxError(token, Language.STXADD_IDENT_WRONG, name);
        }
    }

    /**
     * 从标记中获取有效的标识符
     *
     * @param token 标识符的标记
     * @return 标识符名称字符串
     * @throws SQLException 如果标识符无效则抛出异常
     */
    protected String getIdentifier(SQLToken token) throws SQLException {
        String name = token.getName(sql);
        checkValidIdentifier(name, token);
        return name;
    }

    /**
     * 从标记栈中获取下一个有效的标识符
     *
     * @return 标识符名称字符串
     * @throws SQLException 如果标识符无效则抛出异常
     */
    protected String nextIdentifier() throws SQLException {
        return getIdentifier(nextToken(MISSING_IDENTIFIER));
    }

    /**
     * 检查标识符是否是带有点的两部分名称，如FIRST.SECOND
     *
     * @param name 第一部分的名称
     * @return 如果存在第二部分则返回第二部分，否则返回第一部分
     * @throws SQLException SQL异常
     */
    protected String nextIdentiferPart(String name) throws SQLException {
        SQLToken token = nextToken();
        // 检查对象名称是否包含数据库名称
        if (token != null && token.value == SQLTokenizer.POINT) {
            return nextIdentifier();
        } else {
            previousToken();
        }
        return name;
    }

    /**
     * 判断标记是否为关键字
     * 
     * @param token SQL标记
     * @return 如果是关键字返回true，否则返回false
     */
    final protected boolean isKeyword(SQLToken token) {
        if (token == null)
            return false;
        switch (token.value) {
            case SQLTokenizer.SELECT:
            case SQLTokenizer.INSERT:
            case SQLTokenizer.UPDATE:
            case SQLTokenizer.UNION:
            case SQLTokenizer.FROM:
            case SQLTokenizer.WHERE:
            case SQLTokenizer.GROUP:
            case SQLTokenizer.HAVING:
            case SQLTokenizer.ORDER:
            case SQLTokenizer.COMMA:
            case SQLTokenizer.SET:
            case SQLTokenizer.JOIN:
            case SQLTokenizer.LIMIT:
                return true;
        }
        return false;
    }

    /**
     * 返回方法nextToken返回的最后一个标记
     * 
     * @return SQL标记对象
     */
    protected SQLToken lastToken() {
        if (tokenIdx > tokens.size()) {
            return null;
        }
        return (SQLToken) tokens.get(tokenIdx - 1);
    }

    /**
     * 回退到上一个标记
     */
    protected void previousToken() {
        tokenIdx--;
    }

    /**
     * 获取下一个标记
     * 
     * @return 下一个SQL标记对象
     */
    protected SQLToken nextToken() {
        if (tokenIdx >= tokens.size()) {
            tokenIdx++; // 必须递增，这样previousToken()方法才能正常工作
            return null;
        }
        return (SQLToken) tokens.get(tokenIdx++);
    }

    /**
     * 获取下一个标记并验证其有效性
     * 
     * @param validValues 有效值数组
     * @return SQL标记对象
     * @throws SQLException SQL异常
     */
    protected SQLToken nextToken(int[] validValues) throws SQLException {
        SQLToken token = nextToken();
        if (token == null)
            throw createSyntaxError(token, validValues);
        if (validValues == MISSING_EXPRESSION) {
            return token; // 表达式可以包含在任何标记中
        }
        if (validValues == MISSING_IDENTIFIER) {
            // 以下标记不是有效的标识符
            switch (token.value) {
                case SQLTokenizer.PARENTHESIS_L:
                case SQLTokenizer.PARENTHESIS_R:
                case SQLTokenizer.COMMA:
                    throw createSyntaxError(token, validValues);
            }
            return token;
        }
        for (int i = validValues.length - 1; i >= 0; i--) {
            if (token.value == validValues[i])
                return token;
        }
        throw createSyntaxError(token, validValues);
    }

    /**
     * 解析单个SELECT语句（UNION的一部分或简单的单个SELECT）
     *
     * @return CommandSelect对象
     * @throws SQLException SQL异常
     */
    private CommandSelect singleSelect() throws SQLException {
        CommandSelect selCmd = new CommandSelect(con.log);
        SQLToken token;
        // 扫描前缀，如DISTINCT、ALL和TOP子句；示例：SELECT TOP 15 ...
        Switch:
        while (true) {
            token = nextToken(MISSING_EXPRESSION);
            switch (token.value) {
                case SQLTokenizer.TOP:
                    token = nextToken(MISSING_EXPRESSION);
                    try {
                        int maxRows = Integer.parseInt(token.getName(sql));
                        selCmd.setMaxRows(maxRows);
                    } catch (NumberFormatException e) {
                        throw createSyntaxError(token, Language.STXADD_NOT_NUMBER,
                                token.getName(sql));
                    }
                    break;
                case SQLTokenizer.ALL:
                    selCmd.setDistinct(false);
                    break;
                case SQLTokenizer.DISTINCT:
                    selCmd.setDistinct(true);
                    break;
                default:
                    previousToken();
                    break Switch;
            }
        }

        while (true) {
            Expression column = expression(selCmd, 0);
            selCmd.addColumnExpression(column);

            token = nextToken();
            if (token == null)
                return selCmd; // 没有FROM的SELECT

            boolean as = false;
            if (token.value == SQLTokenizer.AS) {
                token = nextToken(MISSING_EXPRESSION);
                as = true;
            }

            if (as || (!isKeyword(token))) {
                String alias = getIdentifier(token);
                column.setAlias(alias);
                token = nextToken();
                if (token == null)
                    return selCmd; // 没有FROM的SELECT
            }

            switch (token.value) {
                case SQLTokenizer.COMMA:
                    if (column == null)
                        throw createSyntaxError(token, MISSING_EXPRESSION);
                    column = null;
                    break;
                case SQLTokenizer.FROM:
                    if (column == null)
                        throw createSyntaxError(token, MISSING_EXPRESSION);
                    column = null;
                    from(selCmd);
                    return selCmd;

                default:
                    if (!isKeyword(token))
                        throw createSyntaxError(token, new int[]{
                                SQLTokenizer.COMMA, SQLTokenizer.FROM});
                    previousToken();
                    return selCmd;
            }
        }
    }

    /**
     * 解析SELECT语句
     * 
     * @return CommandSelect对象
     * @throws SQLException SQL异常
     */
    final private CommandSelect select() throws SQLException {
        CommandSelect selCmd = singleSelect();
        SQLToken token = nextToken();

        UnionAll union = null;

        while (token != null && token.value == SQLTokenizer.UNION) {
            if (union == null) {
                union = new UnionAll();
                union.addDataSource(new ViewResult(con, selCmd));
                selCmd = new CommandSelect(con.log);
                selCmd.setSource(union);
                DataSources from = new DataSources();
                from.add(union);
                selCmd.setTables(from);
                selCmd.addColumnExpression(new ExpressionName("*"));
            }
            nextToken(MISSING_ALL);
            nextToken(MISSING_SELECT);
            union.addDataSource(new ViewResult(con, singleSelect()));
            token = nextToken();
        }
        if (token != null && token.value == SQLTokenizer.ORDER) {
            order(selCmd);
            token = nextToken();
        }
        if (token != null && token.value == SQLTokenizer.LIMIT) {
            limit(selCmd);
            token = nextToken();
        }
        previousToken();
        return selCmd;
    }

    /**
     * 解析DELETE语句
     * 
     * @return CommandDelete对象
     * @throws SQLException SQL异常
     */
    private Command delete() throws SQLException {
        CommandDelete cmd = new CommandDelete(con.log);
        nextToken(MISSING_FROM);
        from(cmd);
        SQLToken token = nextToken();
        if (token != null) {
            if (token.value != SQLTokenizer.WHERE)
                throw this.createSyntaxError(token, MISSING_WHERE);
            where(cmd);
        }
        return cmd;
    }

    /**
     * 解析TRUNCATE语句
     * 
     * @return CommandDelete对象
     * @throws SQLException SQL异常
     */
    private Command truncate() throws SQLException {
        CommandDelete cmd = new CommandDelete(con.log);
        nextToken(MISSING_TABLE);
        from(cmd);
        return cmd;
    }

    /**
     * 解析INSERT语句
     * 
     * @return CommandInsert对象
     * @throws SQLException SQL异常
     */
    private Command insert() throws SQLException {
        SQLToken token = nextToken(MISSING_INTO);
        CommandInsert cmd = new CommandInsert(con.log, nextIdentifier());

        int parthesisCount = 0;

        token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
        if (token.value == SQLTokenizer.PARENTHESIS_L) {
            token = nextToken(MISSING_EXPRESSION);
            if (token.value == SQLTokenizer.SELECT) {
                parthesisCount++;
                cmd.noColumns = true;
            } else {
                previousToken();
                Expressions list = expressionParenthesisList(cmd);
                for (int i = 0; i < list.size(); i++) {
                    cmd.addColumnExpression(list.get(i));
                }
                token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
            }
        } else
            cmd.noColumns = true;

        Switch:
        while (true)
            switch (token.value) {
                case SQLTokenizer.VALUES: {
                    token = nextToken(MISSING_PARENTHESIS_L);
                    cmd.addValues(expressionParenthesisList(cmd));
                    return cmd;
                }
                case SQLTokenizer.SELECT:
                    cmd.addValues(select());
                    while (parthesisCount-- > 0) {
                        nextToken(MISSING_PARENTHESIS_R);
                    }
                    return cmd;
                case SQLTokenizer.PARENTHESIS_L:
                    token = nextToken(MISSING_PARENTHESIS_VALUES_SELECT);
                    parthesisCount++;
                    continue Switch;
                default:
                    throw new Error();
            }
    }

    /**
     * 解析UPDATE语句
     * 
     * @return CommandUpdate对象
     * @throws SQLException SQL异常
     */
    private Command update() throws SQLException {
        CommandUpdate cmd = new CommandUpdate(con.log);
        // 读取表名
        DataSources tables = new DataSources();
        cmd.setTables(tables);
        cmd.setSource(rowSource(cmd, tables, 0));

        SQLToken token = nextToken(MISSING_SET);
        while (true) {
            token = nextToken();
            Expression dest = expressionSingle(cmd, token);
            if (dest.getType() != Expression.NAME)
                throw createSyntaxError(token, MISSING_IDENTIFIER);
            nextToken(MISSING_EQUALS);
            Expression src = expression(cmd, 0);
            cmd.addSetting(dest, src);
            token = nextToken();
            if (token == null)
                break;
            switch (token.value) {
                case SQLTokenizer.WHERE:
                    where(cmd);
                    return cmd;
                case SQLTokenizer.COMMA:
                    continue;
                default:
                    throw createSyntaxError(token, MISSING_WHERE_COMMA);
            }
        }
        return cmd;
    }

    /**
     * 解析CREATE语句
     * 
     * @return Command对象
     * @throws SQLException SQL异常
     */
    private Command create() throws SQLException {
        while (true) {
            SQLToken token = nextToken(COMMANDS_CREATE);
            switch (token.value) {
                case SQLTokenizer.DATABASE:
                    return createDatabase();
                case SQLTokenizer.TABLE:
                    return createTable();
                case SQLTokenizer.VIEW:
                    return createView();
                case SQLTokenizer.INDEX:
                    return createIndex(false);
                case SQLTokenizer.PROCEDURE:
                    return createProcedure();
                case SQLTokenizer.UNIQUE:
                    do {
                        token = nextToken(COMMANDS_CREATE_UNIQUE);
                    } while (token.value == SQLTokenizer.INDEX);
                    return createIndex(true);
                case SQLTokenizer.NONCLUSTERED:
                case SQLTokenizer.CLUSTERED:
                    continue;
                default:
                    throw createSyntaxError(token, COMMANDS_CREATE);
            }
        }
    }

    /**
     * 解析CREATE DATABASE语句
     * 
     * @return CommandCreateDatabase对象
     * @throws SQLException SQL异常
     */
    private CommandCreateDatabase createDatabase() throws SQLException {
        SQLToken token = nextToken();
        if (token == null)
            throw createSyntaxError(token, MISSING_EXPRESSION);
        return new CommandCreateDatabase(con.log, token.getName(sql));
    }

    /**
     * 解析CREATE TABLE语句
     * 
     * @return CommandTable对象
     * @throws SQLException SQL异常
     */
    private CommandTable createTable() throws SQLException {
        String catalog;
        String tableName = catalog = nextIdentifier();
        tableName = nextIdentiferPart(tableName);
        if (tableName == catalog)
            catalog = null;
        CommandTable cmdCreate = new CommandTable(con.log, catalog, tableName,
                SQLTokenizer.CREATE);
        SQLToken token = nextToken(MISSING_PARENTHESIS_L);

        nextCol:
        while (true) {
            token = nextToken(MISSING_EXPRESSION);

            String constraintName;
            if (token.value == SQLTokenizer.CONSTRAINT) {
                // 读取带名称的约束
                constraintName = nextIdentifier();
                token = nextToken(MISSING_KEYTYPE);
            } else {
                constraintName = null;
            }
            switch (token.value) {
                case SQLTokenizer.PRIMARY:
                case SQLTokenizer.UNIQUE:
                case SQLTokenizer.FOREIGN:
                    IndexDescription index = index(cmdCreate, token.value,
                            tableName, constraintName, null);
                    if (token.value == SQLTokenizer.FOREIGN) {
                        nextToken(MISSING_REFERENCES);
                        String pk = nextIdentifier();
                        Expressions expressions = new Expressions();
                        Strings columns = new Strings();
                        expressionDefList(cmdCreate, expressions, columns);
                        IndexDescription pkIndex = new IndexDescription(null, pk,
                                SQLTokenizer.UNIQUE, expressions, columns);
                        ForeignKey foreignKey = new ForeignKey(pk, pkIndex,
                                tableName, index);
                        cmdCreate.addForeingnKey(foreignKey);
                    } else {
                        cmdCreate.addIndex(index);
                    }

                    token = nextToken(MISSING_COMMA_PARENTHESIS);
                    switch (token.value) {
                        case SQLTokenizer.PARENTHESIS_R:
                            return cmdCreate;
                        case SQLTokenizer.COMMA:
                            continue nextCol;
                    }
            }
            // 标记是列名
            token = addColumn(token, cmdCreate);
            if (token == null) {
                throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
            }
            switch (token.value) {
                case SQLTokenizer.PARENTHESIS_R:
                    return cmdCreate;
                case SQLTokenizer.COMMA:
                    continue nextCol;
                default:
                    throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
            }
        }
    }

    /**
     * 解析列并将其添加到命令中。如果列是唯一或主键，则添加索引。
     *
     * @param token SQLToken包含列名
     * @param cmdCreate CommandTable对象
     * @return 分隔符标记
     */
    private SQLToken addColumn(SQLToken token, CommandTable cmdCreate)
            throws SQLException {
        String colName = getIdentifier(token);
        Column col = datatype(false);
        col.setName(colName);

        token = nextToken();
        boolean nullableWasSet = false;
        boolean defaultWasSet = col.isAutoIncrement(); // 使用数据类型COUNTER时已经设置了此值
        while (true) {
            if (token == null) {
                cmdCreate.addColumn(col);
                return null;
            }
            switch (token.value) {
                case SQLTokenizer.PARENTHESIS_R:
                case SQLTokenizer.COMMA:
                    cmdCreate.addColumn(col);
                    return token;
                case SQLTokenizer.DEFAULT:
                    if (defaultWasSet)
                        throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
                    int offset = token.offset + token.length;
                    token = nextToken();
                    if (token != null)
                        offset = token.offset;
                    previousToken();
                    Expression expr = expression(cmdCreate, 0);
                    SQLToken last = lastToken();
                    int length = last.offset + last.length - offset;
                    String def = new String(sql, offset, length);
                    col.setDefaultValue(expr, def);
                    defaultWasSet = true;
                    break;
                case SQLTokenizer.IDENTITY:
                    if (defaultWasSet)
                        throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
                    col.setAutoIncrement(true);
                    defaultWasSet = true;
                    break;
                case SQLTokenizer.NULL:
                    if (nullableWasSet)
                        throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
                    // col.setNullable(true); 已经是默认值
                    nullableWasSet = true;
                    break;
                case SQLTokenizer.NOT:
                    if (nullableWasSet)
                        throw createSyntaxError(token, MISSING_COMMA_PARENTHESIS);
                    token = nextToken(MISSING_NULL);
                    col.setNullable(false);
                    nullableWasSet = true;
                    break;
                case SQLTokenizer.PRIMARY:
                case SQLTokenizer.UNIQUE:
                    IndexDescription index = index(cmdCreate, token.value,
                            cmdCreate.name, null, colName);
                    cmdCreate.addIndex(index);
                    break;
                default:
                    throw createSyntaxError(token, MISSING_OPTIONS_DATATYPE);
            }
            token = nextToken();
        }
    }

    /**
     * 解析如下结构：<br>
     * <li>PRIMARY KEY (col1) <li>UNIQUE (col1, col2) <li>FOREIGN KEY REFERENCES
     * ref_table(col1)
     *
     * @param cmd 命令对象
     * @param constraintType 约束类型，SQLTokenizer.PRIMARY、SQLTokenizer.UNIQUE或SQLTokenizer.FOREIGN之一
     * @param tableName 表名
     * @param contrainName 约束名称
     * @param columnName 列名
     * @return 新的IndexDescription对象
     */
    private IndexDescription index(Command cmd, int constraintType,
                                   String tableName, String contrainName, String columnName)
            throws SQLException {
        if (constraintType != SQLTokenizer.UNIQUE)
            nextToken(MISSING_KEY);
        SQLToken token = nextToken();
        if (token != null) {
            switch (token.value) {
                case SQLTokenizer.CLUSTERED:
                case SQLTokenizer.NONCLUSTERED:
                    // 忽略，这些来自MS SQL Server的标记被忽略
                    break;
                default:
                    previousToken();
            }
        } else {
            previousToken();
        }
        Strings columns = new Strings();
        Expressions expressions = new Expressions();
        if (columnName != null) {
            // 与列定义一起的单列约束
            columns.add(columnName);
            expressions.add(new ExpressionName(columnName));
        } else {
            // 作为附加定义的约束
            expressionDefList(cmd, expressions, columns);
        }
        return new IndexDescription(contrainName, tableName, constraintType,
                expressions, columns);
    }

    /**
     * 读取数据类型描述。这用于CREATE TABLE和CONVERT函数。
     *
     * @param isEscape 如果为true，则数据类型以"SQL_"开头。这用于转义语法。
     * @return Column对象
     */
    private Column datatype(boolean isEscape) throws SQLException {
        SQLToken token;
        int dataType;
        if (isEscape) {
            token = nextToken(MISSING_SQL_DATATYPE);
            switch (token.value) {
                case SQLTokenizer.SQL_BIGINT:
                    dataType = SQLTokenizer.BIGINT;
                    break;
                case SQLTokenizer.SQL_BINARY:
                    dataType = SQLTokenizer.BINARY;
                    break;
                case SQLTokenizer.SQL_BIT:
                    dataType = SQLTokenizer.BIT;
                    break;
                case SQLTokenizer.SQL_CHAR:
                    dataType = SQLTokenizer.CHAR;
                    break;
                case SQLTokenizer.SQL_DATE:
                    dataType = SQLTokenizer.DATE;
                    break;
                case SQLTokenizer.SQL_DECIMAL:
                    dataType = SQLTokenizer.DECIMAL;
                    break;
                case SQLTokenizer.SQL_DOUBLE:
                    dataType = SQLTokenizer.DOUBLE;
                    break;
                case SQLTokenizer.SQL_FLOAT:
                    dataType = SQLTokenizer.FLOAT;
                    break;
                case SQLTokenizer.SQL_INTEGER:
                    dataType = SQLTokenizer.INT;
                    break;
                case SQLTokenizer.SQL_LONGVARBINARY:
                    dataType = SQLTokenizer.LONGVARBINARY;
                    break;
                case SQLTokenizer.SQL_LONGVARCHAR:
                    dataType = SQLTokenizer.LONGVARCHAR;
                    break;
                case SQLTokenizer.SQL_REAL:
                    dataType = SQLTokenizer.REAL;
                    break;
                case SQLTokenizer.SQL_SMALLINT:
                    dataType = SQLTokenizer.SMALLINT;
                    break;
                case SQLTokenizer.SQL_TIME:
                    dataType = SQLTokenizer.TIME;
                    break;
                case SQLTokenizer.SQL_TIMESTAMP:
                    dataType = SQLTokenizer.TIMESTAMP;
                    break;
                case SQLTokenizer.SQL_TINYINT:
                    dataType = SQLTokenizer.TINYINT;
                    break;
                case SQLTokenizer.SQL_VARBINARY:
                    dataType = SQLTokenizer.VARBINARY;
                    break;
                case SQLTokenizer.SQL_VARCHAR:
                    dataType = SQLTokenizer.VARCHAR;
                    break;
                default:
                    throw new Error();
            }
        } else {
            token = nextToken(MISSING_DATATYPE);
            dataType = token.value;
        }
        Column col = new Column();

        // 两部分数据类型
        if (dataType == SQLTokenizer.LONG) {
            token = nextToken();
            if (token != null && token.value == SQLTokenizer.RAW) {
                dataType = SQLTokenizer.LONGVARBINARY;
            } else {
                dataType = SQLTokenizer.LONGVARCHAR;
                previousToken();
            }
        }

        switch (dataType) {
            case SQLTokenizer.RAW:
                dataType = SQLTokenizer.VARBINARY;
                // no break;
            case SQLTokenizer.CHAR:
            case SQLTokenizer.VARCHAR:
            case SQLTokenizer.NCHAR:
            case SQLTokenizer.NVARCHAR:
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY: {
                // 检测最大列大小
                token = nextToken();
                int displaySize;
                if (token == null || token.value != SQLTokenizer.PARENTHESIS_L) {
                    displaySize = 30;
                    previousToken();
                } else {
                    token = nextToken(MISSING_EXPRESSION);
                    try {
                        displaySize = Integer.parseInt(token.getName(sql));
                    } catch (Exception e) {
                        throw createSyntaxError(token, MISSING_NUMBERVALUE);
                    }
                    nextToken(MISSING_PARENTHESIS_R);
                }
                col.setPrecision(displaySize);
                break;
            }
            case SQLTokenizer.SYSNAME:
                col.setPrecision(255);
                dataType = SQLTokenizer.VARCHAR;
                break;
            case SQLTokenizer.COUNTER:
                col.setAutoIncrement(true);
                dataType = SQLTokenizer.INT;
                break;
            case SQLTokenizer.NUMERIC:
            case SQLTokenizer.DECIMAL:
                token = nextToken();
                if (token != null && token.value == SQLTokenizer.PARENTHESIS_L) {
                    // 读取数据类型的精度
                    token = nextToken(MISSING_EXPRESSION);
                    int value;
                    try {
                        value = Integer.parseInt(token.getName(sql));
                    } catch (Exception e) {
                        throw createSyntaxError(token, MISSING_NUMBERVALUE);
                    }
                    col.setPrecision(value);
                    token = nextToken(MISSING_COMMA_PARENTHESIS);
                    if (token.value == SQLTokenizer.COMMA) {
                        // 读取数据类型的标度
                        token = nextToken(MISSING_EXPRESSION);
                        try {
                            value = Integer.parseInt(token.getName(sql));
                        } catch (Exception e) {
                            throw createSyntaxError(token, MISSING_NUMBERVALUE);
                        }
                        col.setScale(value);
                        nextToken(MISSING_PARENTHESIS_R);
                    }
                } else {
                    col.setPrecision(18); // decimal和numeric的默认精度
                    previousToken();
                }
                break;
        }
        col.setDataType(dataType);
        return col;
    }

    /**
     * 解析CREATE VIEW语句
     * 
     * @return CommandCreateView对象
     * @throws SQLException SQL异常
     */
    private CommandCreateView createView() throws SQLException {
        String viewName = nextIdentifier();

        nextToken(MISSING_AS);
        SQLToken token = nextToken(MISSING_SELECT);
        CommandCreateView cmd = new CommandCreateView(con.log, viewName);

        cmd.sql = new String(sql, token.offset, sql.length - token.offset);
        select(); // 解析以检查有效性
        return cmd;
    }

    /**
     * 解析CREATE INDEX语句
     * 
     * @param unique 是否唯一索引
     * @return CommandTable对象
     * @throws SQLException SQL异常
     */
    private CommandTable createIndex(boolean unique) throws SQLException {
        String indexName = nextIdentifier();
        nextToken(MISSING_ON);
        String catalog;
        String tableName = catalog = nextIdentifier();
        tableName = nextIdentiferPart(tableName);
        if (tableName == catalog)
            catalog = null;
        CommandTable cmd = new CommandTable(con.log, catalog, tableName,
                SQLTokenizer.INDEX);
        Expressions expressions = new Expressions();
        Strings columns = new Strings();
        expressionDefList(cmd, expressions, columns);
        IndexDescription indexDesc = new IndexDescription(indexName, tableName,
                unique ? SQLTokenizer.UNIQUE : SQLTokenizer.INDEX, expressions,
                columns);
        // TODO Create Index
        Object[] param = {"Create Index"};
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
    }

    /**
     * 解析CREATE PROCEDURE语句
     * 
     * @return CommandCreateDatabase对象
     * @throws SQLException SQL异常
     */
    private CommandCreateDatabase createProcedure() throws SQLException {
        // TODO Create Procedure
        Object[] param = {"Create Procedure"};
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, param);
    }

    /**
     * 解析DROP语句
     * 
     * @return Command对象
     * @throws SQLException SQL异常
     */
    private Command drop() throws SQLException {
        SQLToken tokenType = nextToken(COMMANDS_DROP);

        String catalog;
        String name = catalog = nextIdentifier();
        name = nextIdentiferPart(name);
        if (name == catalog)
            catalog = null;

        switch (tokenType.value) {
            case SQLTokenizer.DATABASE:
            case SQLTokenizer.TABLE:
            case SQLTokenizer.VIEW:
            case SQLTokenizer.INDEX:
            case SQLTokenizer.PROCEDURE:
                return new CommandDrop(con.log, catalog, name, tokenType.value);
            default:
                throw createSyntaxError(tokenType, COMMANDS_DROP);
        }
    }

    /**
     * 解析ALTER语句
     * 
     * @return Command对象
     * @throws SQLException SQL异常
     */
    private Command alter() throws SQLException {
        SQLToken tokenType = nextToken(COMMANDS_ALTER);
        String catalog;
        String tableName = catalog = nextIdentifier();
        switch (tokenType.value) {
            case SQLTokenizer.TABLE:
            case SQLTokenizer.VIEW:
            case SQLTokenizer.INDEX:
            case SQLTokenizer.PROCEDURE:
                tableName = nextIdentiferPart(tableName);
                if (tableName == catalog)
                    catalog = null;
        }
        switch (tokenType.value) {
            // case SQLTokenizer.DATABASE:
            case SQLTokenizer.TABLE:
                return alterTable(catalog, tableName);
            // case SQLTokenizer.VIEW:
            // case SQLTokenizer.INDEX:
            // case SQLTokenizer.PROCEDURE:
            default:
                Object[] param = {"ALTER " + tokenType.getName(sql)};
                throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION,
                        param);
        }
    }

    /**
     * 解析ALTER TABLE语句
     * 
     * @param catalog 数据库目录名
     * @param name 表名
     * @return Command对象
     * @throws SQLException SQL异常
     */
    Command alterTable(String catalog, String name) throws SQLException {
        SQLToken tokenType = nextToken(MISSING_ADD_ALTER_DROP);
        CommandTable cmd = new CommandTable(con.log, catalog, name,
                tokenType.value);
        switch (tokenType.value) {
            case SQLTokenizer.ADD:
                SQLToken token;
                do {
                    token = nextToken(MISSING_IDENTIFIER);
                    token = addColumn(token, cmd);
                } while (token != null && token.value == SQLTokenizer.COMMA);

                return cmd;
            default:
                Object[] param = {"ALTER TABLE " + tokenType.getName(sql)};
                throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION,
                        param);
        }
    }

    /**
     * 解析SET语句
     * 
     * @return Command对象
     * @throws SQLException SQL异常
     */
    private CommandSet set() throws SQLException {
        SQLToken token = nextToken(COMMANDS_SET);
        switch (token.value) {
            case SQLTokenizer.TRANSACTION:
                return setTransaction();
            default:
                throw new Error();
        }
    }

    /**
     * 解析SET TRANSACTION语句
     * 
     * @return CommandSet对象
     * @throws SQLException SQL异常
     */
    private CommandSet setTransaction() throws SQLException {
        SQLToken token = nextToken(MISSING_ISOLATION);
        token = nextToken(MISSING_LEVEL);
        token = nextToken(COMMANDS_TRANS_LEVEL);
        CommandSet cmd = new CommandSet(con.log, SQLTokenizer.LEVEL);
        switch (token.value) {
            case SQLTokenizer.READ:
                token = nextToken(MISSING_COMM_UNCOMM);
                switch (token.value) {
                    case SQLTokenizer.COMMITTED:
                        cmd.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
                        break;
                    case SQLTokenizer.UNCOMMITTED:
                        cmd.isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
                        break;
                    default:
                        throw new Error();
                }
                return cmd;
            case SQLTokenizer.REPEATABLE:
                token = nextToken(MISSING_READ);
                cmd.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
                return cmd;
            case SQLTokenizer.SERIALIZABLE:
                cmd.isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
                return cmd;
            default:
                throw new Error();
        }

    }

    /**
     * 解析EXECUTE语句
     * 
     * @return Command对象
     * @throws SQLException SQL异常
     */
    private Command execute() throws SQLException {
        // TODO Execute
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION,
                "Execute");
    }

    /**
     * 读取括号中的表达式列表，如VALUES()或函数。左括号已被消耗。
     *
     * @param cmd 需要添加参数"?"的命令对象
     * @return 表达式列表
     * @see #expressionDefList
     */
    private Expressions expressionParenthesisList(Command cmd)
            throws SQLException {
        Expressions list = new Expressions();
        {
            SQLToken token = nextToken();
            if (token != null && token.value == SQLTokenizer.PARENTHESIS_R) {
                // 空列表，如没有参数的函数
                return list;
            }
            previousToken();
        }
        while (true) {
            list.add(expression(cmd, 0));
            SQLToken token = nextToken(MISSING_COMMA_PARENTHESIS);
            switch (token.value) {
                case SQLTokenizer.PARENTHESIS_R:
                    return list;
                case SQLTokenizer.COMMA:
                    continue;
                default:
                    throw new Error();
            }
        }
    }

    /**
     * 读取表达式列表。列表由特定SQL关键字（如SELECT、GROUP BY、ORDER BY）限定
     * 
     * @param cmd 命令对象
     * @param listType 列表类型
     * @return 表达式列表
     * @throws SQLException SQL异常
     */
    private Expressions expressionTokenList(Command cmd, int listType)
            throws SQLException {
        Expressions list = new Expressions();
        while (true) {
            Expression expr = expression(cmd, 0);
            list.add(expr);
            SQLToken token = nextToken();

            if (listType == SQLTokenizer.ORDER && token != null) {
                switch (token.value) {
                    case SQLTokenizer.DESC:
                        expr.setAlias(SQLTokenizer.DESC_STR);
                        // no break;
                    case SQLTokenizer.ASC:
                        token = nextToken();
                }
            }

            if (token == null) {
                previousToken();
                return list;
            }

            switch (token.value) {
                case SQLTokenizer.COMMA:
                    continue;
                default:
                    if (isKeyword(token)) {
                        previousToken();
                        return list;
                    }
                    throw createSyntaxError(token, MISSING_TOKEN_LIST);
            }
        }
    }

    /**
     * 读取表达式定义列表
     * 
     * @param cmd 命令对象
     * @param expressions 表达式列表
     * @param columns 列名列表
     * @throws SQLException SQL异常
     */
    private void expressionDefList(Command cmd, Expressions expressions,
                                   Strings columns) throws SQLException {
        SQLToken token = nextToken();
        if (token.value != SQLTokenizer.PARENTHESIS_L)
            throw createSyntaxError(token, MISSING_PARENTHESIS_L);
        Loop:
        while (true) {
            int offset = token.offset + token.length;
            token = nextToken();
            if (token != null)
                offset = token.offset;
            previousToken();

            expressions.add(expression(cmd, 0));
            SQLToken last = lastToken();
            int length = last.offset + last.length - offset;
            columns.add(new String(sql, offset, length));

            token = nextToken(MISSING_COMMA_PARENTHESIS);
            switch (token.value) {
                case SQLTokenizer.PARENTHESIS_R:
                    break Loop;
                case SQLTokenizer.COMMA:
                    continue;
                default:
                    throw new Error();
            }
        }
    }

    /**
     * 读取可由多个原子表达式构建的复杂表达式。
     *
     * @param cmd 命令对象，用于添加参数"?"
     * @param previousOperationLevel 左操作的级别
     * @return 表达式对象
     */
    private Expression expression(Command cmd, int previousOperationLevel)
            throws SQLException {
        SQLToken token = nextToken(MISSING_EXPRESSION);
        Expression leftExpr;
        switch (token.value) {
            case SQLTokenizer.NOT:
                leftExpr = new ExpressionArithmetic(expression(cmd,
                        ExpressionArithmetic.NOT / 10), ExpressionArithmetic.NOT);
                break;
            case SQLTokenizer.MINUS:
                leftExpr = new ExpressionArithmetic(expression(cmd,
                        ExpressionArithmetic.NEGATIVE / 10),
                        ExpressionArithmetic.NEGATIVE);
                break;
            case SQLTokenizer.TILDE:
                leftExpr = new ExpressionArithmetic(expression(cmd,
                        ExpressionArithmetic.BIT_NOT / 10),
                        ExpressionArithmetic.BIT_NOT);
                break;
            case SQLTokenizer.PARENTHESIS_L:
                leftExpr = expression(cmd, 0);
                token = nextToken(MISSING_PARENTHESIS_R);
                break;
            default:
                leftExpr = expressionSingle(cmd, token);
        }
        boolean isNot = false;
        while ((token = nextToken()) != null) {
            Expression rightExpr;
            int operation = ExpressionArithmetic
                    .getOperationFromToken(token.value);
            int level = operation / 10;
            if (previousOperationLevel >= level) {
                previousToken();
                return leftExpr;
            }
            switch (token.value) {
                case SQLTokenizer.PLUS:
                case SQLTokenizer.MINUS:
                case SQLTokenizer.ASTERISK:
                case SQLTokenizer.SLACH:
                case SQLTokenizer.PERCENT:
                case SQLTokenizer.EQUALS:
                case SQLTokenizer.LESSER:
                case SQLTokenizer.LESSER_EQU:
                case SQLTokenizer.GREATER:
                case SQLTokenizer.GREATER_EQU:
                case SQLTokenizer.UNEQUALS:
                case SQLTokenizer.LIKE:
                case SQLTokenizer.OR:
                case SQLTokenizer.AND:
                case SQLTokenizer.BIT_AND:
                case SQLTokenizer.BIT_OR:
                case SQLTokenizer.BIT_XOR:
                    rightExpr = expression(cmd, level);
                    leftExpr = new ExpressionArithmetic(leftExpr, rightExpr,
                            operation);
                    break;
                case SQLTokenizer.BETWEEN:
                    rightExpr = expression(cmd, ExpressionArithmetic.AND);
                    nextToken(MISSING_AND);
                    Expression rightExpr2 = expression(cmd, level);
                    leftExpr = new ExpressionArithmetic(leftExpr, rightExpr,
                            rightExpr2, operation);
                    break;
                case SQLTokenizer.IN:
                    nextToken(MISSING_PARENTHESIS_L);
                    token = nextToken(MISSING_EXPRESSION);
                    if (token.value == SQLTokenizer.SELECT) {
                        CommandSelect cmdSel = select();
                        leftExpr = new ExpressionInSelect(con, leftExpr, cmdSel,
                                operation);
                        nextToken(MISSING_PARENTHESIS_R);
                    } else {
                        previousToken();
                        Expressions list = expressionParenthesisList(cmd);
                        leftExpr = new ExpressionArithmetic(leftExpr, list,
                                operation);
                    }
                    break;
                case SQLTokenizer.IS:
                    token = nextToken(MISSING_NOT_NULL);
                    if (token.value == SQLTokenizer.NOT) {
                        nextToken(MISSING_NULL);
                        operation++;
                    }
                    leftExpr = new ExpressionArithmetic(leftExpr, operation);
                    break;
                case SQLTokenizer.NOT:
                    token = nextToken(MISSING_BETWEEN_IN);
                    previousToken();
                    isNot = true;
                    continue;
                default:
                    previousToken();
                    return leftExpr;
            }
            if (isNot) {
                isNot = false;
                leftExpr = new ExpressionArithmetic(leftExpr,
                        ExpressionArithmetic.NOT);
            }
        }
        previousToken();
        return leftExpr;
    }

    /**
     * 此方法解析单个表达式，如12、'qwert'、0x3F或列名。
     *
     * @param cmd 需要添加参数"?"的命令对象
     * @param token SQL标记
     * @return 表达式对象
     */
    private Expression expressionSingle(Command cmd, SQLToken token)
            throws SQLException {
        boolean isMinus = false;
        if (token != null) {
            switch (token.value) {
                case SQLTokenizer.NULL:
                    return new ExpressionValue(null, SQLTokenizer.NULL);
                case SQLTokenizer.STRING:
                    return new ExpressionValue(token.getName(null),
                            SQLTokenizer.VARCHAR);
                case SQLTokenizer.IDENTIFIER: {
                    String name = getIdentifier(token);
                    ExpressionName expr = new ExpressionName(name);
                    SQLToken token2 = nextToken();
                    if (token2 != null && token2.value == SQLTokenizer.POINT) {
                        expr.setNameAfterTableAlias(nextIdentifier());
                    } else {
                        previousToken();
                    }
                    return expr;
                }
                case SQLTokenizer.TRUE:
                    return new ExpressionValue(Boolean.TRUE, SQLTokenizer.BOOLEAN);
                case SQLTokenizer.FALSE:
                    return new ExpressionValue(Boolean.FALSE, SQLTokenizer.BOOLEAN);
                case SQLTokenizer.ESCAPE_L: {
                    token = nextToken(COMMANDS_ESCAPE);
                    SQLToken para = nextToken(MISSING_EXPRESSION);
                    Expression expr;
                    switch (token.value) {
                        case SQLTokenizer.D: // 日期转义序列
                            expr = new ExpressionValue(DateTime.valueOf(
                                    para.getName(sql), SQLTokenizer.DATE),
                                    SQLTokenizer.DATE);
                            break;
                        case SQLTokenizer.T: // 时间转义序列
                            expr = new ExpressionValue(DateTime.valueOf(
                                    para.getName(sql), SQLTokenizer.TIME),
                                    SQLTokenizer.TIME);
                            break;
                        case SQLTokenizer.TS: // 时间戳转义序列
                            expr = new ExpressionValue(DateTime.valueOf(
                                    para.getName(sql), SQLTokenizer.TIMESTAMP),
                                    SQLTokenizer.TIMESTAMP);
                            break;
                        case SQLTokenizer.FN: // 函数转义序列
                            nextToken(MISSING_PARENTHESIS_L);
                            expr = function(cmd, para, true);
                            break;
                        case SQLTokenizer.CALL: // 调用转义序列
                            throw new java.lang.UnsupportedOperationException(
                                    "call escape sequence");
                        default:
                            throw new Error();
                    }
                    token = nextToken(ESCAPE_MISSING_CLOSE);
                    return expr;
                }
                case SQLTokenizer.QUESTION:
                    ExpressionValue param = new ExpressionValue();
                    cmd.addParameter(param);
                    return param;
                case SQLTokenizer.CASE:
                    return caseExpr(cmd);
                case SQLTokenizer.MINUS:
                case SQLTokenizer.PLUS:
                    // 符号检测
                    do {
                        if (token.value == SQLTokenizer.MINUS)
                            isMinus = !isMinus;
                        token = nextToken();
                        if (token == null)
                            throw createSyntaxError(token, MISSING_EXPRESSION);
                    } while (token.value == SQLTokenizer.MINUS
                            || token.value == SQLTokenizer.PLUS);
                    // no Break
                default:
                    SQLToken token2 = nextToken();
                    if (token2 != null
                            && token2.value == SQLTokenizer.PARENTHESIS_L) {
                        if (isMinus)
                            return new ExpressionArithmetic(function(cmd, token,
                                    false), ExpressionArithmetic.NEGATIVE);
                        return function(cmd, token, false);
                    } else {
                        // 常量表达式或标识符
                        char chr1 = sql[token.offset];
                        if (chr1 == '$') {
                            previousToken();
                            String tok = new String(sql, token.offset + 1,
                                    token.length - 1);
                            if (isMinus)
                                tok = "-" + tok;
                            return new ExpressionValue(new Money(
                                    Double.parseDouble(tok)), SQLTokenizer.MONEY);
                        }
                        String tok = new String(sql, token.offset, token.length);
                        if ((chr1 >= '0' && '9' >= chr1) || chr1 == '.') {
                            previousToken();
                            // 第一个字符是数字
                            if (token.length > 1
                                    && (sql[token.offset + 1] | 0x20) == 'x') {
                                // 二进制数据作为十六进制
                                if (isMinus) {
                                    throw createSyntaxError(token,
                                            Language.STXADD_OPER_MINUS);
                                }
                                return new ExpressionValue(Utils.hex2bytes(sql,
                                        token.offset + 2, token.length - 2),
                                        SQLTokenizer.VARBINARY);
                            }
                            if (isMinus)
                                tok = "-" + tok;
                            if (Utils.indexOf('.', sql, token.offset, token.length) >= 0
                                    || Utils.indexOf('e', sql, token.offset,
                                    token.length) >= 0) {
                                return new ExpressionValue(new Double(tok),
                                        SQLTokenizer.DOUBLE);
                            } else {
                                try {
                                    return new ExpressionValue(new Integer(tok),
                                            SQLTokenizer.INT);
                                } catch (NumberFormatException e) {
                                    return new ExpressionValue(new Long(tok),
                                            SQLTokenizer.BIGINT);
                                }
                            }
                        } else {
                            // 标识符
                            checkValidIdentifier(tok, token);
                            ExpressionName expr = new ExpressionName(tok);
                            if (token2 != null
                                    && token2.value == SQLTokenizer.POINT) {
                                expr.setNameAfterTableAlias(nextIdentifier());
                            } else {
                                previousToken();
                            }
                            if (isMinus)
                                return new ExpressionArithmetic(expr,
                                        ExpressionArithmetic.NEGATIVE);
                            return expr;
                        }
                    }
            }
        }
        return null;
    }

    /**
     * 解析CASE表达式
     * 
     * @param cmd 命令对象
     * @return ExpressionFunctionCase对象
     * @throws SQLException SQL异常
     */
    ExpressionFunctionCase caseExpr(final Command cmd) throws SQLException {
        ExpressionFunctionCase expr = new ExpressionFunctionCase();
        SQLToken token = nextToken(MISSING_EXPRESSION);

        Expression input = null;
        if (token.value != SQLTokenizer.WHEN) {
            // 简单CASE语法
            previousToken();
            input = expression(cmd, 0);
            token = nextToken(MISSING_WHEN_ELSE_END);
        }

        while (true) {
            switch (token.value) {
                case SQLTokenizer.WHEN:
                    Expression condition = expression(cmd, 0);
                    if (input != null) {
                        // 简单CASE语法
                        condition = new ExpressionArithmetic(input, condition,
                                ExpressionArithmetic.EQUALS);
                    }
                    nextToken(MISSING_THEN);
                    Expression result = expression(cmd, 0);
                    expr.addCase(condition, result);
                    break;
                case SQLTokenizer.ELSE:
                    expr.setElseResult(expression(cmd, 0));
                    break;
                case SQLTokenizer.END:
                    expr.setEnd();
                    return expr;
                default:
                    throw new Error();
            }
            token = nextToken(MISSING_WHEN_ELSE_END);
        }
    }

    /**
     * 解析任何函数。左括号已从标记列表中消耗。
     *
     * @param cmd 命令对象
     * @param token 函数的SQLToken
     * @param isEscape 如果函数是FN ESCAPE序列
     * @return 表达式对象
     */
    private Expression function(Command cmd, SQLToken token, boolean isEscape)
            throws SQLException {
        Expression expr;
        switch (token.value) {
            case SQLTokenizer.CONVERT: {
                Column col;
                Expression style = null;
                if (isEscape) {
                    expr = expression(cmd, 0);
                    nextToken(MISSING_COMMA);
                    col = datatype(isEscape);
                } else {
                    col = datatype(isEscape);
                    nextToken(MISSING_COMMA);
                    expr = expression(cmd, 0);
                    token = nextToken(MISSING_COMMA_PARENTHESIS);
                    if (token.value == SQLTokenizer.COMMA) {
                        style = expression(cmd, 0);
                    } else
                        previousToken();
                }
                nextToken(MISSING_PARENTHESIS_R);
                return new ExpressionFunctionConvert(col, expr, style);
            }
            case SQLTokenizer.CAST:
                expr = expression(cmd, 0);
                nextToken(MISSING_AS);
                Column col = datatype(false);
                nextToken(MISSING_PARENTHESIS_R);
                return new ExpressionFunctionConvert(col, expr, null);
            case SQLTokenizer.TIMESTAMPDIFF:
                token = nextToken(MISSING_INTERVALS);
                nextToken(MISSING_COMMA);
                expr = expression(cmd, 0);
                nextToken(MISSING_COMMA);
                expr = new ExpressionFunctionTimestampDiff(token.value, expr,
                        expression(cmd, 0));
                nextToken(MISSING_PARENTHESIS_R);
                return expr;
            case SQLTokenizer.TIMESTAMPADD:
                token = nextToken(MISSING_INTERVALS);
                nextToken(MISSING_COMMA);
                expr = expression(cmd, 0);
                nextToken(MISSING_COMMA);
                expr = new ExpressionFunctionTimestampAdd(token.value, expr,
                        expression(cmd, 0));
                nextToken(MISSING_PARENTHESIS_R);
                return expr;
        }
        Expressions paramList = expressionParenthesisList(cmd);
        int paramCount = paramList.size();
        Expression[] params = paramList.toArray();
        boolean invalidParamCount;
        switch (token.value) {
            // 数值函数:
            case SQLTokenizer.ABS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionAbs();
                break;
            case SQLTokenizer.ACOS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionACos();
                break;
            case SQLTokenizer.ASIN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionASin();
                break;
            case SQLTokenizer.ATAN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionATan();
                break;
            case SQLTokenizer.ATAN2:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionATan2();
                break;
            case SQLTokenizer.CEILING:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCeiling();
                break;
            case SQLTokenizer.COS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCos();
                break;
            case SQLTokenizer.COT:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCot();
                break;
            case SQLTokenizer.DEGREES:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionDegrees();
                break;
            case SQLTokenizer.EXP:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionExp();
                break;
            case SQLTokenizer.FLOOR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionFloor();
                break;
            case SQLTokenizer.LOG:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLog();
                break;
            case SQLTokenizer.LOG10:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLog10();
                break;
            case SQLTokenizer.MOD:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionMod();
                break;
            case SQLTokenizer.PI:
                invalidParamCount = (paramCount != 0);
                expr = new ExpressionFunctionPI();
                break;
            case SQLTokenizer.POWER:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionPower();
                break;
            case SQLTokenizer.RADIANS:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionRadians();
                break;
            case SQLTokenizer.RAND:
                invalidParamCount = (paramCount != 0) && (paramCount != 1);
                expr = new ExpressionFunctionRand();
                break;
            case SQLTokenizer.ROUND:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionRound();
                break;
            case SQLTokenizer.SIN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSin();
                break;
            case SQLTokenizer.SIGN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSign();
                break;
            case SQLTokenizer.SQRT:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSqrt();
                break;
            case SQLTokenizer.TAN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionTan();
                break;
            case SQLTokenizer.TRUNCATE:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionTruncate();
                break;

            // 字符串函数:
            case SQLTokenizer.ASCII:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionAscii();
                break;
            case SQLTokenizer.BITLEN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionBitLen();
                break;
            case SQLTokenizer.CHARLEN:
            case SQLTokenizer.CHARACTLEN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionCharLen();
                break;
            case SQLTokenizer.CHAR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionChar();
                break;
            case SQLTokenizer.CONCAT:
                if (paramCount != 2) {
                    invalidParamCount = true;
                    expr = null;// only for compiler
                    break;
                }
                invalidParamCount = false;
                expr = new ExpressionArithmetic(params[0], params[1],
                        ExpressionArithmetic.ADD);
                break;
            case SQLTokenizer.DIFFERENCE:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionDifference();
                break;
            case SQLTokenizer.INSERT:
                invalidParamCount = (paramCount != 4);
                expr = new ExpressionFunctionInsert();
                break;
            case SQLTokenizer.LCASE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLCase();
                break;
            case SQLTokenizer.LEFT:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionLeft();
                break;
            case SQLTokenizer.LENGTH:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLength();
                break;
            case SQLTokenizer.LOCATE:
                invalidParamCount = (paramCount != 2) && (paramCount != 3);
                expr = new ExpressionFunctionLocate();
                break;
            case SQLTokenizer.LTRIM:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionLTrim();
                break;
            case SQLTokenizer.OCTETLEN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionOctetLen();
                break;
            case SQLTokenizer.REPEAT:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionRepeat();
                break;
            case SQLTokenizer.REPLACE:
                invalidParamCount = (paramCount != 3);
                expr = new ExpressionFunctionReplace();
                break;
            case SQLTokenizer.RIGHT:
                invalidParamCount = (paramCount != 2);
                expr = new ExpressionFunctionRight();
                break;
            case SQLTokenizer.RTRIM:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionRTrim();
                break;
            case SQLTokenizer.SPACE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSpace();
                break;
            case SQLTokenizer.SOUNDEX:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionSoundex();
                break;
            case SQLTokenizer.SUBSTRING:
                invalidParamCount = (paramCount != 3);
                expr = new ExpressionFunctionSubstring();
                break;
            case SQLTokenizer.UCASE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionUCase();
                break;

            // 日期时间函数
            case SQLTokenizer.CURDATE:
            case SQLTokenizer.CURRENTDATE:
                invalidParamCount = (paramCount != 0);
                expr = new ExpressionValue(new DateTime(DateTime.now(),
                        SQLTokenizer.DATE), SQLTokenizer.DATE);
                break;
            case SQLTokenizer.CURTIME:
                invalidParamCount = (paramCount != 0);
                expr = new ExpressionValue(new DateTime(DateTime.now(),
                        SQLTokenizer.TIME), SQLTokenizer.TIME);
                break;
            case SQLTokenizer.DAYOFMONTH:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionDayOfMonth();
                break;
            case SQLTokenizer.DAYOFWEEK:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionDayOfWeek();
                break;
            case SQLTokenizer.DAYOFYEAR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionDayOfYear();
                break;
            case SQLTokenizer.HOUR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionHour();
                break;
            case SQLTokenizer.MINUTE:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionMinute();
                break;
            case SQLTokenizer.MONTH:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionMonth();
                break;
            case SQLTokenizer.NOW:
                invalidParamCount = (paramCount != 0);
                expr = new ExpressionValue(new DateTime(DateTime.now(),
                        SQLTokenizer.TIMESTAMP), SQLTokenizer.TIMESTAMP);
                break;
            case SQLTokenizer.YEAR:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionFunctionYear();
                break;

            // 系统函数:
            case SQLTokenizer.IIF:
                invalidParamCount = (paramCount != 3);
                expr = new ExpressionFunctionIIF();
                break;
            case SQLTokenizer.SWITCH:
                invalidParamCount = (paramCount % 2 != 0);
                ExpressionFunctionCase exprCase = new ExpressionFunctionCase();
                for (int i = 0; i < paramCount - 1; i += 2)
                    exprCase.addCase(params[i], params[i + 1]);
                exprCase.setEnd();
                expr = exprCase;
                break;
            case SQLTokenizer.IFNULL:
                switch (paramCount) {
                    case 1:
                        return new ExpressionArithmetic(params[0],
                                ExpressionArithmetic.ISNULL);
                    case 2:
                        invalidParamCount = false;
                        expr = new ExpressionFunctionIIF();
                        Expression[] newParams = new Expression[3];
                        newParams[0] = new ExpressionArithmetic(params[0],
                                ExpressionArithmetic.ISNULL);
                        newParams[1] = params[1];
                        newParams[2] = params[0];
                        params = newParams;
                        paramCount = 3;
                        break;
                    default:
                        invalidParamCount = true;
                        expr = null; // only for Compiler
                }
                break;

            // 聚合函数
            case SQLTokenizer.COUNT:
                invalidParamCount = (paramCount != 1);
                if (params[0].getType() == Expression.NAME) {
                    // 检测特殊情况COUNT(*)
                    ExpressionName param = (ExpressionName) params[0];
                    if ("*".equals(param.getName())
                            && param.getTableAlias() == null) {
                        // 设置任何非NULL值作为参数
                        params[0] = new ExpressionValue("*", SQLTokenizer.VARCHAR);
                    }
                }
                expr = new ExpressionName(Expression.COUNT);
                break;
            case SQLTokenizer.SUM:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionName(Expression.SUM);
                break;
            case SQLTokenizer.MAX:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionName(Expression.MAX);
                break;
            case SQLTokenizer.MIN:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionName(Expression.MIN);
                break;
            case SQLTokenizer.FIRST:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionName(Expression.FIRST);
                break;
            case SQLTokenizer.LAST:
                invalidParamCount = (paramCount != 1);
                expr = new ExpressionName(Expression.LAST);
                break;
            case SQLTokenizer.AVG:
                if (paramCount != 1) {
                    invalidParamCount = true;
                    expr = null;// Only for the compiler
                    break;
                }
                expr = new ExpressionName(Expression.SUM);
                expr.setParams(params);
                Expression expr2 = new ExpressionName(Expression.COUNT);
                expr2.setParams(params);
                expr = new ExpressionArithmetic(expr, expr2,
                        ExpressionArithmetic.DIV);
                return expr;
            default:
                throw createSyntaxError(token, Language.STXADD_FUNC_UNKNOWN);
        }
        if (invalidParamCount) {
            throw createSyntaxError(token, Language.STXADD_PARAM_INVALID_COUNT);
        }
        expr.setParams(params);
        return expr;
    }

    /**
     * 读取FROM子句中的表或视图名称。如果存在关键字AS，则也读取别名
     * 
     * @param cmd 命令对象
     * @param tables 数据源列表
     * @return RowSource对象
     * @throws SQLException SQL异常
     */
    private RowSource tableSource(Command cmd, DataSources tables)
            throws SQLException {
        SQLToken token = nextToken(MISSING_EXPRESSION);
        switch (token.value) {
            case SQLTokenizer.PARENTHESIS_L: // (
                return rowSource(cmd, tables, SQLTokenizer.PARENTHESIS_R);
            case SQLTokenizer.ESCAPE_L: // {
                token = nextToken(MISSING_OJ);
                return rowSource(cmd, tables, SQLTokenizer.ESCAPE_R);
            case SQLTokenizer.SELECT:
                // 内部select
                ViewResult viewResult = new ViewResult(con, select());
                tables.add(viewResult);
                return viewResult;
        }
        String catalog = null;
        String name = getIdentifier(token);
        token = nextToken();
        // 检查表名是否包含数据库名
        if (token != null && token.value == SQLTokenizer.POINT) {
            catalog = name;
            name = nextIdentifier();
            token = nextToken();
        }
        // TableResult table = new TableResult();
        // table.setName( catalog, name );
        View tableView = Database.getTableView(con, catalog, name);
        TableViewResult table = TableViewResult.createResult(tableView);
        tables.add(table);

        if (token != null && token.value == SQLTokenizer.AS) {
            // 跳过AS关键字（如果存在）
            token = nextToken(MISSING_EXPRESSION);
            table.setAlias(token.getName(sql));
        } else {
            previousToken();
        }
        return table;
    }

    /**
     * 读取from子句中的连接
     * 
     * @param cmd 命令对象
     * @param tables 数据源列表
     * @param left 左侧RowSource
     * @param type 连接类型
     * @return Join对象
     * @throws SQLException SQL异常
     */
    private Join join(Command cmd, DataSources tables, RowSource left, int type)
            throws SQLException {
        RowSource right = rowSource(cmd, tables, 0);
        SQLToken token = nextToken();

        while (true) {
            if (token == null) {
                throw createSyntaxError(token, Language.STXADD_JOIN_INVALID);
            }

            switch (token.value) {
                case SQLTokenizer.ON:
                    if (type == Join.RIGHT_JOIN)
                        return new Join(Join.LEFT_JOIN, right, left, expression(
                                cmd, 0));
                    return new Join(type, left, right, expression(cmd, 0));
                default:
                    if (!right.hasAlias()) {
                        right.setAlias(token.getName(sql));
                        token = nextToken();
                        continue;
                    }
                    throw createSyntaxError(token, MISSING_ON);
            }
        }
    }

    /**
     * 返回行源。行源可以是表、连接、视图或行函数。
     * 
     * @param cmd 命令对象
     * @param tables 数据源列表
     * @param parenthesis 括号类型
     * @return RowSource对象
     * @throws SQLException SQL异常
     */
    private RowSource rowSource(Command cmd, DataSources tables, int parenthesis)
            throws SQLException {
        RowSource fromSource = null;
        fromSource = tableSource(cmd, tables);

        while (true) {
            SQLToken token = nextToken();
            if (token == null)
                return fromSource;
            switch (token.value) {
                case SQLTokenizer.ON:
                    previousToken();
                    return fromSource;
                case SQLTokenizer.CROSS:
                    nextToken(MISSING_JOIN);
                    // no break
                case SQLTokenizer.COMMA:
                    fromSource = new Join(Join.CROSS_JOIN, fromSource, rowSource(
                            cmd, tables, 0), null);
                    break;
                case SQLTokenizer.INNER:
                    nextToken(MISSING_JOIN);
                    // no break;
                case SQLTokenizer.JOIN:
                    fromSource = join(cmd, tables, fromSource, Join.INNER_JOIN);
                    break;
                case SQLTokenizer.LEFT:
                    token = nextToken(MISSING_OUTER_JOIN);
                    if (token.value == SQLTokenizer.OUTER)
                        token = nextToken(MISSING_JOIN);
                    fromSource = join(cmd, tables, fromSource, Join.LEFT_JOIN);
                    break;
                case SQLTokenizer.RIGHT:
                    token = nextToken(MISSING_OUTER_JOIN);
                    if (token.value == SQLTokenizer.OUTER)
                        token = nextToken(MISSING_JOIN);
                    fromSource = join(cmd, tables, fromSource, Join.RIGHT_JOIN);
                    break;
                case SQLTokenizer.FULL:
                    token = nextToken(MISSING_OUTER_JOIN);
                    if (token.value == SQLTokenizer.OUTER)
                        token = nextToken(MISSING_JOIN);
                    fromSource = join(cmd, tables, fromSource, Join.FULL_JOIN);
                    break;
                case SQLTokenizer.PARENTHESIS_R:
                case SQLTokenizer.ESCAPE_R:
                    if (parenthesis == token.value)
                        return fromSource;
                    if (parenthesis == 0) {
                        previousToken();
                        return fromSource;
                    }
                    throw createSyntaxError(token, Language.STXADD_FROM_PAR_CLOSE);
                default:
                    if (isKeyword(token)) {
                        previousToken();
                        return fromSource;
                    }
                    if (!fromSource.hasAlias()) {
                        fromSource.setAlias(token.getName(sql));
                        break;
                    }
                    throw createSyntaxError(token, new int[]{SQLTokenizer.COMMA,
                            SQLTokenizer.GROUP, SQLTokenizer.ORDER,
                            SQLTokenizer.HAVING});
            }
        }
    }

    /**
     * 解析FROM子句
     * 
     * @param cmd CommandSelect对象
     * @throws SQLException SQL异常
     */
    private void from(CommandSelect cmd) throws SQLException {
        DataSources tables = new DataSources();
        cmd.setTables(tables);
        cmd.setSource(rowSource(cmd, tables, 0));

        SQLToken token;
        while (null != (token = nextToken())) {
            switch (token.value) {
                case SQLTokenizer.WHERE:
                    where(cmd);
                    break;
                case SQLTokenizer.GROUP:
                    group(cmd);
                    break;
                case SQLTokenizer.HAVING:
                    having(cmd);
                    break;
                default:
                    previousToken();
                    return;
            }
        }
    }

    /**
     * 解析ORDER BY子句
     * 
     * @param cmd CommandSelect对象
     * @throws SQLException SQL异常
     */
    private void order(CommandSelect cmd) throws SQLException {
        nextToken(MISSING_BY);
        cmd.setOrder(expressionTokenList(cmd, SQLTokenizer.ORDER));
    }

    /**
     * 解析LIMIT子句
     * 
     * @param selCmd CommandSelect对象
     * @throws SQLException SQL异常
     */
    private void limit(CommandSelect selCmd) throws SQLException {
        SQLToken token = nextToken(MISSING_EXPRESSION);
        try {
            int maxRows = Integer.parseInt(token.getName(sql));
            selCmd.setMaxRows(maxRows);
        } catch (NumberFormatException e) {
            throw createSyntaxError(token, Language.STXADD_NOT_NUMBER,
                    token.getName(sql));
        }
    }

    /**
     * 解析GROUP BY子句
     * 
     * @param cmd CommandSelect对象
     * @throws SQLException SQL异常
     */
    private void group(CommandSelect cmd) throws SQLException {
        nextToken(MISSING_BY);
        cmd.setGroup(expressionTokenList(cmd, SQLTokenizer.GROUP));
    }

    /**
     * 解析WHERE子句
     * 
     * @param cmd CommandSelect对象
     * @throws SQLException SQL异常
     */
    private void where(CommandSelect cmd) throws SQLException {
        cmd.setWhere(expression(cmd, 0));
    }

    /**
     * 解析HAVING子句
     * 
     * @param cmd CommandSelect对象
     * @throws SQLException SQL异常
     */
    private void having(CommandSelect cmd) throws SQLException {
        cmd.setHaving(expression(cmd, 0));
    }

    // 各种命令和标记的常量数组定义
    private static final int[] COMMANDS = {SQLTokenizer.SELECT,
            SQLTokenizer.DELETE, SQLTokenizer.INSERT, SQLTokenizer.UPDATE,
            SQLTokenizer.CREATE, SQLTokenizer.DROP, SQLTokenizer.ALTER,
            SQLTokenizer.SET, SQLTokenizer.USE, SQLTokenizer.EXECUTE,
            SQLTokenizer.TRUNCATE};
    private static final int[] COMMANDS_ESCAPE = {SQLTokenizer.D,
            SQLTokenizer.T, SQLTokenizer.TS, SQLTokenizer.FN, SQLTokenizer.CALL};
    private static final int[] COMMANDS_ALTER = {SQLTokenizer.DATABASE,
            SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.PROCEDURE,};
    private static final int[] COMMANDS_CREATE = {SQLTokenizer.DATABASE,
            SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.INDEX,
            SQLTokenizer.PROCEDURE, SQLTokenizer.UNIQUE,
            SQLTokenizer.CLUSTERED, SQLTokenizer.NONCLUSTERED};
    private static final int[] COMMANDS_DROP = {SQLTokenizer.DATABASE,
            SQLTokenizer.TABLE, SQLTokenizer.VIEW, SQLTokenizer.INDEX,
            SQLTokenizer.PROCEDURE};
    private static final int[] COMMANDS_SET = {SQLTokenizer.TRANSACTION};
    private static final int[] COMMANDS_CREATE_UNIQUE = {SQLTokenizer.INDEX,
            SQLTokenizer.CLUSTERED, SQLTokenizer.NONCLUSTERED};
    private static final int[] MISSING_TABLE = {SQLTokenizer.TABLE};
    private static final int[] ESCAPE_MISSING_CLOSE = {SQLTokenizer.ESCAPE_R};
    private static final int[] MISSING_EXPRESSION = {SQLTokenizer.VALUE};
    private static final int[] MISSING_IDENTIFIER = {SQLTokenizer.IDENTIFIER};
    private static final int[] MISSING_BY = {SQLTokenizer.BY};
    private static final int[] MISSING_PARENTHESIS_L = {SQLTokenizer.PARENTHESIS_L};
    private static final int[] MISSING_PARENTHESIS_R = {SQLTokenizer.PARENTHESIS_R};
    private static final int[] MISSING_DATATYPE = {SQLTokenizer.BIT,
            SQLTokenizer.BOOLEAN, SQLTokenizer.BINARY, SQLTokenizer.VARBINARY,
            SQLTokenizer.RAW, SQLTokenizer.LONGVARBINARY, SQLTokenizer.BLOB,
            SQLTokenizer.TINYINT, SQLTokenizer.SMALLINT, SQLTokenizer.INT,
            SQLTokenizer.COUNTER, SQLTokenizer.BIGINT, SQLTokenizer.SMALLMONEY,
            SQLTokenizer.MONEY, SQLTokenizer.DECIMAL, SQLTokenizer.NUMERIC,
            SQLTokenizer.REAL, SQLTokenizer.FLOAT, SQLTokenizer.DOUBLE,
            SQLTokenizer.DATE, SQLTokenizer.TIME, SQLTokenizer.TIMESTAMP,
            SQLTokenizer.SMALLDATETIME, SQLTokenizer.CHAR, SQLTokenizer.NCHAR,
            SQLTokenizer.VARCHAR, SQLTokenizer.NVARCHAR, SQLTokenizer.LONG,
            SQLTokenizer.LONGNVARCHAR, SQLTokenizer.LONGVARCHAR,
            SQLTokenizer.CLOB, SQLTokenizer.NCLOB,
            SQLTokenizer.UNIQUEIDENTIFIER, SQLTokenizer.JAVA_OBJECT,
            SQLTokenizer.SYSNAME};
    private static final int[] MISSING_SQL_DATATYPE = {
            SQLTokenizer.SQL_BIGINT, SQLTokenizer.SQL_BINARY,
            SQLTokenizer.SQL_BIT, SQLTokenizer.SQL_CHAR, SQLTokenizer.SQL_DATE,
            SQLTokenizer.SQL_DECIMAL, SQLTokenizer.SQL_DOUBLE,
            SQLTokenizer.SQL_FLOAT, SQLTokenizer.SQL_INTEGER,
            SQLTokenizer.SQL_LONGVARBINARY, SQLTokenizer.SQL_LONGVARCHAR,
            SQLTokenizer.SQL_REAL, SQLTokenizer.SQL_SMALLINT,
            SQLTokenizer.SQL_TIME, SQLTokenizer.SQL_TIMESTAMP,
            SQLTokenizer.SQL_TINYINT, SQLTokenizer.SQL_VARBINARY,
            SQLTokenizer.SQL_VARCHAR};
    private static final int[] MISSING_INTO = {SQLTokenizer.INTO};
    private static final int[] MISSING_BETWEEN_IN = {SQLTokenizer.BETWEEN,
            SQLTokenizer.IN};
    private static final int[] MISSING_NOT_NULL = {SQLTokenizer.NOT,
            SQLTokenizer.NULL};
    private static final int[] MISSING_NULL = {SQLTokenizer.NULL};
    private static final int[] MISSING_COMMA = {SQLTokenizer.COMMA};
    private static final int[] MISSING_COMMA_PARENTHESIS = {
            SQLTokenizer.COMMA, SQLTokenizer.PARENTHESIS_R};
    private static final int[] MISSING_PARENTHESIS_VALUES_SELECT = {
            SQLTokenizer.PARENTHESIS_L, SQLTokenizer.VALUES,
            SQLTokenizer.SELECT};
    private static final int[] MISSING_TOKEN_LIST = {SQLTokenizer.COMMA,
            SQLTokenizer.FROM, SQLTokenizer.GROUP, SQLTokenizer.HAVING,
            SQLTokenizer.ORDER};
    private static final int[] MISSING_FROM = {SQLTokenizer.FROM};
    private static final int[] MISSING_SET = {SQLTokenizer.SET};
    private static final int[] MISSING_EQUALS = {SQLTokenizer.EQUALS};
    private static final int[] MISSING_WHERE = {SQLTokenizer.WHERE};
    private static final int[] MISSING_WHERE_COMMA = {SQLTokenizer.WHERE,
            SQLTokenizer.COMMA};
    private static final int[] MISSING_ISOLATION = {SQLTokenizer.ISOLATION};
    private static final int[] MISSING_LEVEL = {SQLTokenizer.LEVEL};
    private static final int[] COMMANDS_TRANS_LEVEL = {SQLTokenizer.READ,
            SQLTokenizer.REPEATABLE, SQLTokenizer.SERIALIZABLE};
    private static final int[] MISSING_READ = {SQLTokenizer.READ};
    private static final int[] MISSING_COMM_UNCOMM = {SQLTokenizer.COMMITTED,
            SQLTokenizer.UNCOMMITTED};
    private static final int[] MISSING_OPTIONS_DATATYPE = {
            SQLTokenizer.DEFAULT, SQLTokenizer.IDENTITY, SQLTokenizer.NOT,
            SQLTokenizer.NULL, SQLTokenizer.PRIMARY, SQLTokenizer.UNIQUE,
            SQLTokenizer.COMMA, SQLTokenizer.PARENTHESIS_R};
    private static final int[] MISSING_NUMBERVALUE = {SQLTokenizer.NUMBERVALUE};
    private static final int[] MISSING_AND = {SQLTokenizer.AND};
    private static final int[] MISSING_JOIN = {SQLTokenizer.JOIN};
    private static final int[] MISSING_OUTER_JOIN = {SQLTokenizer.OUTER,
            SQLTokenizer.JOIN};
    private static final int[] MISSING_OJ = {SQLTokenizer.OJ};
    private static final int[] MISSING_ON = {SQLTokenizer.ON};
    private static final int[] MISSING_KEYTYPE = {SQLTokenizer.PRIMARY,
            SQLTokenizer.UNIQUE, SQLTokenizer.FOREIGN};
    private static final int[] MISSING_KEY = {SQLTokenizer.KEY};
    private static final int[] MISSING_REFERENCES = {SQLTokenizer.REFERENCES};
    private static final int[] MISSING_AS = {SQLTokenizer.AS};
    private static final int[] MISSING_SELECT = {SQLTokenizer.SELECT};
    private static final int[] MISSING_INTERVALS = {
            SQLTokenizer.SQL_TSI_FRAC_SECOND, SQLTokenizer.SQL_TSI_SECOND,
            SQLTokenizer.SQL_TSI_MINUTE, SQLTokenizer.SQL_TSI_HOUR,
            SQLTokenizer.SQL_TSI_DAY, SQLTokenizer.SQL_TSI_WEEK,
            SQLTokenizer.SQL_TSI_MONTH, SQLTokenizer.SQL_TSI_QUARTER,
            SQLTokenizer.SQL_TSI_YEAR, SQLTokenizer.MILLISECOND,
            SQLTokenizer.SECOND, SQLTokenizer.MINUTE, SQLTokenizer.HOUR,
            SQLTokenizer.DAY, SQLTokenizer.WEEK, SQLTokenizer.MONTH,
            SQLTokenizer.QUARTER, SQLTokenizer.YEAR, SQLTokenizer.D};
    private static final int[] MISSING_ALL = {SQLTokenizer.ALL};
    private static final int[] MISSING_THEN = {SQLTokenizer.THEN};
    private static final int[] MISSING_WHEN_ELSE_END = {SQLTokenizer.WHEN,
            SQLTokenizer.ELSE, SQLTokenizer.END};
    private static final int[] MISSING_ADD_ALTER_DROP = {SQLTokenizer.ADD,
            SQLTokenizer.ALTER, SQLTokenizer.DROP};

}