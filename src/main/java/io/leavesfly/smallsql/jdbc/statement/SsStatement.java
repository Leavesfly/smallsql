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
 * SSStatement.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.jdbc.statement;

import java.sql.*;
import java.util.ArrayList;

import io.leavesfly.smallsql.rdb.sql.SQLParser;
import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.command.Command;

/**
 * SmallSQL 数据库 Statement 实现类。
 * <p>
 * 这个类实现了 java.sql.Statement 接口，用于执行静态 SQL 语句并返回结果。
 * 它提供了执行查询、更新、批处理等操作的方法。
 * <p>
 * Statement 对象不是线程安全的，不应该在多个线程之间共享。
 */
public class SsStatement implements Statement {

    /**
     * 关联的数据库连接
     */
    public final SsConnection con;

    /**
     * 执行的命令对象
     */
    Command cmd;

    /**
     * Statement 是否已关闭
     */
    private boolean isClosed;

    /**
     * 结果集类型
     */
    public int rsType;

    /**
     * 结果集并发性
     */
    public int rsConcurrency;

    /**
     * 结果集获取方向
     */
    private int fetchDirection;

    /**
     * 结果集获取大小
     */
    private int fetchSize;

    /**
     * 查询超时时间（秒）
     */
    private int queryTimeout;

    /**
     * 最大行数
     */
    private int maxRows;

    /**
     * 最大字段大小
     */
    private int maxFieldSize;

    /**
     * 批处理 SQL 语句列表
     */
    private ArrayList batches;

    /**
     * 是否需要生成键
     */
    private boolean needGeneratedKeys;

    /**
     * 生成的键结果集
     */
    private ResultSet generatedKeys;

    /**
     * 生成键的索引数组
     */
    private int[] generatedKeyIndexes;

    /**
     * 生成键的名称数组
     */
    private String[] generatedKeyNames;

    /**
     * 构造一个新的 Statement 对象
     *
     * @param con 数据库连接
     * @throws SQLException 如果创建过程中发生错误
     */
    public SsStatement(SsConnection con) throws SQLException {
        this(con, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * 构造一个新的 Statement 对象，指定结果集类型和并发性
     *
     * @param con 数据库连接
     * @param rsType 结果集类型
     * @param rsConcurrency 结果集并发性
     * @throws SQLException 如果创建过程中发生错误
     */
    public SsStatement(SsConnection con, int rsType, int rsConcurrency)
            throws SQLException {
        this.con = con;
        this.rsType = rsType;
        this.rsConcurrency = rsConcurrency;
        con.testClosedConnection();
    }

    /**
     * 执行 SQL 查询语句并返回结果集
     *
     * @param sql 要执行的 SQL 查询语句
     * @return 包含查询结果的 ResultSet 对象
     * @throws SQLException 如果执行过程中发生错误
     */
    final public ResultSet executeQuery(String sql) throws SQLException {
        executeImpl(sql);
        return cmd.getQueryResult();
    }

    /**
     * 执行 SQL 更新语句（INSERT、UPDATE、DELETE）
     *
     * @param sql 要执行的 SQL 更新语句
     * @return 受影响的行数
     * @throws SQLException 如果执行过程中发生错误
     */
    final public int executeUpdate(String sql) throws SQLException {
        executeImpl(sql);
        return cmd.getUpdateCount();
    }

    /**
     * 执行任意 SQL 语句
     *
     * @param sql 要执行的 SQL 语句
     * @return 如果执行结果是 ResultSet 则返回 true，否则返回 false
     * @throws SQLException 如果执行过程中发生错误
     */
    final public boolean execute(String sql) throws SQLException {
        executeImpl(sql);
        return cmd.getResultSet() != null;
    }

    /**
     * 执行 SQL 语句的实际实现方法
     *
     * @param sql 要执行的 SQL 语句
     * @throws SQLException 如果执行过程中发生错误
     */
    final private void executeImpl(String sql) throws SQLException {
        checkStatement();
        generatedKeys = null;
        try {
            con.log.println(sql);
            SQLParser parser = new SQLParser();
            cmd = parser.parse(con, sql);
            if (maxRows != 0
                    && (cmd.getMaxRows() == -1 || cmd.getMaxRows() > maxRows))
                cmd.setMaxRows(maxRows);
            cmd.execute(con, this);
        } catch (Exception e) {
            throw SmallSQLException.createFromException(e);
        }
        needGeneratedKeys = false;
        generatedKeyIndexes = null;
        generatedKeyNames = null;
    }

    /**
     * 关闭 Statement 对象并释放相关资源
     */
    final public void close() {
        con.log.println("Statement.close");
        isClosed = true;
        cmd = null;
        // TODO make Resources free;
    }

    /**
     * 获取最大字段大小
     *
     * @return 最大字段大小（字节）
     */
    final public int getMaxFieldSize() {
        return maxFieldSize;
    }

    /**
     * 设置最大字段大小
     *
     * @param max 最大字段大小（字节）
     */
    final public void setMaxFieldSize(int max) {
        maxFieldSize = max;
    }

    /**
     * 获取最大行数限制
     *
     * @return 最大行数，0 表示无限制
     */
    final public int getMaxRows() {
        return maxRows;
    }

    /**
     * 设置最大行数限制
     *
     * @param max 最大行数，0 表示无限制
     * @throws SQLException 如果设置的值为负数
     */
    final public void setMaxRows(int max) throws SQLException {
        if (max < 0)
            throw SmallSQLException.create(Language.ROWS_WRONG_MAX,
                    String.valueOf(max));
        maxRows = max;
    }

    /**
     * 启用或禁用转义处理
     *
     * @param enable true 启用转义处理，false 禁用转义处理
     * @throws SQLException 如果操作过程中发生错误
     */
    final public void setEscapeProcessing(boolean enable) throws SQLException {
        checkStatement();
        // TODO enable/disable escape processing
    }

    /**
     * 获取查询超时时间
     *
     * @return 查询超时时间（秒）
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getQueryTimeout() throws SQLException {
        checkStatement();
        return queryTimeout;
    }

    /**
     * 设置查询超时时间
     *
     * @param seconds 查询超时时间（秒）
     * @throws SQLException 如果操作过程中发生错误
     */
    final public void setQueryTimeout(int seconds) throws SQLException {
        checkStatement();
        queryTimeout = seconds;
    }

    /**
     * 取消执行当前语句
     *
     * @throws SQLException 如果操作过程中发生错误
     */
    final public void cancel() throws SQLException {
        checkStatement();
        // TODO Statement.cancel()
    }

    /**
     * 获取警告信息
     *
     * @return SQLWarning 对象，如果没有警告则返回 null
     */
    final public SQLWarning getWarnings() {
        return null;
    }

    /**
     * 清除警告信息
     */
    final public void clearWarnings() {
        // TODO support for warnings
    }

    /**
     * 设置游标名称（用于定位更新）
     *
     * @param name 游标名称
     * @throws SQLException 如果操作过程中发生错误
     */
    final public void setCursorName(String name) throws SQLException {
        /** @todo: Implement this java.sql.Statement.setCursorName method */
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION,
                "setCursorName");
    }

    /**
     * 获取当前结果集
     *
     * @return 当前 ResultSet 对象，如果没有则返回 null
     * @throws SQLException 如果操作过程中发生错误
     */
    final public ResultSet getResultSet() throws SQLException {
        checkStatement();
        return cmd.getResultSet();
    }

    /**
     * 获取更新计数
     *
     * @return 受影响的行数，如果没有则返回 -1
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getUpdateCount() throws SQLException {
        checkStatement();
        return cmd.getUpdateCount();
    }

    /**
     * 检查是否还有更多结果
     *
     * @return 如果还有更多结果返回 true，否则返回 false
     * @throws SQLException 如果操作过程中发生错误
     */
    final public boolean getMoreResults() throws SQLException {
        checkStatement();
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    /**
     * 设置结果集获取方向
     *
     * @param direction 获取方向
     * @throws SQLException 如果操作过程中发生错误
     */
    final public void setFetchDirection(int direction) throws SQLException {
        checkStatement();
        fetchDirection = direction;
    }

    /**
     * 获取结果集获取方向
     *
     * @return 获取方向
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getFetchDirection() throws SQLException {
        checkStatement();
        return fetchDirection;
    }

    /**
     * 设置结果集获取大小
     *
     * @param rows 获取大小
     * @throws SQLException 如果操作过程中发生错误
     */
    final public void setFetchSize(int rows) throws SQLException {
        checkStatement();
        fetchSize = rows;
    }

    /**
     * 获取结果集获取大小
     *
     * @return 获取大小
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getFetchSize() throws SQLException {
        checkStatement();
        return fetchSize;
    }

    /**
     * 获取结果集并发性
     *
     * @return 并发性
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getResultSetConcurrency() throws SQLException {
        checkStatement();
        return rsConcurrency;
    }

    /**
     * 获取结果集类型
     *
     * @return 结果集类型
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getResultSetType() throws SQLException {
        checkStatement();
        return rsType;
    }

    /**
     * 添加一个批处理 SQL 语句
     *
     * @param sql 要添加的 SQL 语句
     */
    final public void addBatch(String sql) {
        if (batches == null)
            batches = new ArrayList();
        batches.add(sql);
    }

    /**
     * 清除所有批处理 SQL 语句
     *
     * @throws SQLException 如果操作过程中发生错误
     */
    public void clearBatch() throws SQLException {
        checkStatement();
        if (batches == null)
            return;
        batches.clear();
    }

    /**
     * 执行所有批处理 SQL 语句
     *
     * @return 包含每个 SQL 语句执行结果的数组
     * @throws BatchUpdateException 如果执行过程中发生错误
     */
    public int[] executeBatch() throws BatchUpdateException {
        if (batches == null)
            return new int[0];
        final int[] result = new int[batches.size()];
        BatchUpdateException failed = null;
        for (int i = 0; i < result.length; i++) {
            try {
                result[i] = executeUpdate((String) batches.get(i));
            } catch (SQLException ex) {
                result[i] = EXECUTE_FAILED;
                if (failed == null) {
                    failed = new BatchUpdateException(ex.getMessage(),
                            ex.getSQLState(), ex.getErrorCode(), result);
                    failed.initCause(ex);
                }
                failed.setNextException(ex);
            }
        }
        batches.clear();
        if (failed != null)
            throw failed;
        return result;
    }

    /**
     * 获取关联的数据库连接
     *
     * @return 数据库连接对象
     */
    final public Connection getConnection() {
        return con;
    }

    /**
     * 检查是否还有更多结果
     *
     * @param current 处理当前结果的方式
     * @return 如果还有更多结果返回 true，否则返回 false
     * @throws SQLException 如果操作过程中发生错误
     */
    final public boolean getMoreResults(int current) throws SQLException {
        switch (current) {
            case CLOSE_ALL_RESULTS:
                // currently there exists only one ResultSet
            case CLOSE_CURRENT_RESULT:
                ResultSet rs = cmd.getResultSet();
                cmd.rs = null;
                if (rs != null)
                    rs.close();
                break;
            case KEEP_CURRENT_RESULT:
                break;
            default:
                throw SmallSQLException.create(Language.FLAGVALUE_INVALID,
                        String.valueOf(current));
        }
        return cmd.getMoreResults();
    }

    /**
     * 设置是否需要生成键
     *
     * @param autoGeneratedKeys 是否需要生成键
     * @throws SQLException 如果设置的值无效
     */
    public final void setNeedGeneratedKeys(int autoGeneratedKeys) throws SQLException {
        switch (autoGeneratedKeys) {
            case NO_GENERATED_KEYS:
                break;
            case RETURN_GENERATED_KEYS:
                needGeneratedKeys = true;
                break;
            default:
                throw SmallSQLException.create(Language.ARGUMENT_INVALID,
                        String.valueOf(autoGeneratedKeys));
        }
    }

    /**
     * 设置是否需要生成键
     *
     * @param columnIndexes 生成键的列索引数组
     * @throws SQLException 如果设置的值无效
     */
    public final void setNeedGeneratedKeys(int[] columnIndexes) throws SQLException {
        needGeneratedKeys = columnIndexes != null;
        generatedKeyIndexes = columnIndexes;
    }

    /**
     * 设置是否需要生成键
     *
     * @param columnNames 生成键的列名称数组
     * @throws SQLException 如果设置的值无效
     */
    public final void setNeedGeneratedKeys(String[] columnNames) throws SQLException {
        needGeneratedKeys = columnNames != null;
        generatedKeyNames = columnNames;
    }

    /**
     * 检查是否需要生成键
     *
     * @return 如果需要生成键返回 true，否则返回 false
     */
    public final boolean needGeneratedKeys() {
        return needGeneratedKeys;
    }

    /**
     * 获取生成键的列索引数组
     *
     * @return 生成键的列索引数组
     */
    public final int[] getGeneratedKeyIndexes() {
        return generatedKeyIndexes;
    }

    /**
     * 获取生成键的列名称数组
     *
     * @return 生成键的列名称数组
     */
    public final String[] getGeneratedKeyNames() {
        return generatedKeyNames;
    }

    /**
     * Set on execution the result with the generated keys.
     *
     * @param rs
     */
    public final void setGeneratedKeys(ResultSet rs) {
        generatedKeys = rs;
    }

    /**
     * 获取生成键的结果集
     *
     * @return 生成键的结果集
     * @throws SQLException 如果操作过程中发生错误
     */
    final public ResultSet getGeneratedKeys() throws SQLException {
        if (generatedKeys == null)
            throw SmallSQLException.create(Language.GENER_KEYS_UNREQUIRED);
        return generatedKeys;
    }

    /**
     * 执行 SQL 更新语句并返回生成键
     *
     * @param sql 要执行的 SQL 更新语句
     * @param autoGeneratedKeys 是否需要生成键
     * @return 受影响的行数
     * @throws SQLException 如果执行过程中发生错误
     */
    final public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        setNeedGeneratedKeys(autoGeneratedKeys);
        return executeUpdate(sql);
    }

    /**
     * 执行 SQL 更新语句并返回生成键
     *
     * @param sql 要执行的 SQL 更新语句
     * @param columnIndexes 生成键的列索引数组
     * @return 受影响的行数
     * @throws SQLException 如果执行过程中发生错误
     */
    final public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        setNeedGeneratedKeys(columnIndexes);
        return executeUpdate(sql);
    }

    /**
     * 执行 SQL 更新语句并返回生成键
     *
     * @param sql 要执行的 SQL 更新语句
     * @param columnNames 生成键的列名称数组
     * @return 受影响的行数
     * @throws SQLException 如果执行过程中发生错误
     */
    final public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        setNeedGeneratedKeys(columnNames);
        return executeUpdate(sql);
    }

    /**
     * 执行任意 SQL 语句并返回生成键
     *
     * @param sql 要执行的 SQL 语句
     * @param autoGeneratedKeys 是否需要生成键
     * @return 如果执行结果是 ResultSet 则返回 true，否则返回 false
     * @throws SQLException 如果执行过程中发生错误
     */
    final public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        setNeedGeneratedKeys(autoGeneratedKeys);
        return execute(sql);
    }

    /**
     * 执行任意 SQL 语句并返回生成键
     *
     * @param sql 要执行的 SQL 语句
     * @param columnIndexes 生成键的列索引数组
     * @return 如果执行结果是 ResultSet 则返回 true，否则返回 false
     * @throws SQLException 如果执行过程中发生错误
     */
    final public boolean execute(String sql, int[] columnIndexes)
            throws SQLException {
        setNeedGeneratedKeys(columnIndexes);
        return execute(sql);
    }

    /**
     * 执行任意 SQL 语句并返回生成键
     *
     * @param sql 要执行的 SQL 语句
     * @param columnNames 生成键的列名称数组
     * @return 如果执行结果是 ResultSet 则返回 true，否则返回 false
     * @throws SQLException 如果执行过程中发生错误
     */
    final public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        setNeedGeneratedKeys(columnNames);
        return execute(sql);
    }

    /**
     * 获取结果集保持性
     *
     * @return 结果集保持性
     * @throws SQLException 如果操作过程中发生错误
     */
    final public int getResultSetHoldability() throws SQLException {
        /** @todo: Implement this java.sql.Statement method */
        throw new java.lang.UnsupportedOperationException(
                "Method getResultSetHoldability() not yet implemented.");
    }

    /**
     * 检查 Statement 是否已关闭
     *
     * @throws SQLException 如果 Statement 已关闭
     */
    void checkStatement() throws SQLException {
        if (isClosed) {
            throw SmallSQLException.create(Language.STMT_IS_CLOSED);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isPoolable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }


    public void closeOnCompletion() throws SQLException {
        // TODO Auto-generated method stub

    }

    public boolean isCloseOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }
}