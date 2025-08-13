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
 * SSConnection.java
 * ---------------
 * Author: Volker Berlin
 *
 */
package io.leavesfly.smallsql.jdbc;

import java.nio.channels.FileChannel;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import io.leavesfly.smallsql.jdbc.metadata.SsDatabaseMetaData;
import io.leavesfly.smallsql.jdbc.statement.SsCallableStatement;
import io.leavesfly.smallsql.jdbc.statement.SsStatement;
import io.leavesfly.smallsql.jdbc.statement.SsPreparedStatement;
import io.leavesfly.smallsql.jdbc.statement.SsSavepoint;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.logger.Logger;
import io.leavesfly.smallsql.rdb.engine.Database;
import io.leavesfly.smallsql.rdb.engine.TransactionStep;

/**
 * SmallSQL 数据库连接实现类。
 * <p>
 * 这个类实现了 java.sql.Connection 接口，代表与 SmallSQL 数据库的一个连接。
 * 它负责管理事务、语句创建、元数据访问等功能。
 * <p>
 * 一个连接可以被多个线程共享，但不能同时使用。连接对象不是线程安全的。
 */
public class SsConnection implements Connection {

    /**
     * 连接是否为只读模式
     */
    private boolean readOnly;
    
    /**
     * 连接的数据库实例
     */
    private Database database;
    
    /**
     * 是否自动提交事务，默认为 true
     */
    private boolean autoCommit = true;

    // see also getDefaultTransactionIsolation
    /**
     * 事务隔离级别，默认为 TRANSACTION_READ_COMMITTED
     */
    public int isolationLevel = TRANSACTION_READ_COMMITTED;
    
    /**
     * 事务步骤列表，用于事务提交或回滚
     */
    private List<TransactionStep> commitPages = new ArrayList<TransactionStep>();
    
    /**
     * 事务开始的时间
     */
    private long transactionTime;
    
    /**
     * 数据库元数据对象
     */
    private final SsDatabaseMetaData metadata;
    
    /**
     * 结果集的可保持性
     */
    private int holdability;
    
    /**
     * 日志记录器
     */
    public final Logger log;

    /**
     * 构造一个新的数据库连接
     *
     * @param props 连接属性，包括数据库路径、只读模式、是否创建数据库等
     * @throws SQLException 如果连接过程中发生错误
     */
    public SsConnection(Properties props) throws SQLException {
        SmallSQLException.setLanguage(props.get("locale"));
        log = new Logger();
        String name = props.getProperty("dbpath");
        readOnly = "true".equals(props.getProperty("readonly"));
        boolean create = "true".equals(props.getProperty("create"));
        database = Database.getDatabase(name, this, create);
        metadata = new SsDatabaseMetaData(this);
    }

    /**
     * 创建一个连接的副本，拥有自己的事务空间
     *
     * @param con 原始连接
     */
    public SsConnection(SsConnection con) {
        readOnly = con.readOnly;
        database = con.database;
        metadata = con.metadata;
        log = con.log;
    }

    /**
     * 获取连接的数据库实例
     *
     * @param returnNull 如果未连接到数据库，是否返回 null
     * @throws SQLException 如果未连接到数据库且 returnNull 为 false
     */
    public Database getDatabase(boolean returnNull) throws SQLException {
        testClosedConnection();
        if (!returnNull && database == null)
            throw SmallSQLException.create(Language.DB_NOTCONNECTED);
        return database;
    }

    /**
     * 获取用于连接同步块的监视器对象
     * 多次调用返回相同的对象
     *
     * @return 此连接的唯一对象
     */
    public Object getMonitor() {
        return this;
    }

    /**
     * 创建 Statement 对象用于执行 SQL 语句
     *
     * @return 新创建的 Statement 对象
     * @throws SQLException 如果创建过程中发生错误
     */
    @Override
    public Statement createStatement() throws SQLException {
        return new SsStatement(this);
    }

    /**
     * 创建 PreparedStatement 对象用于执行预编译的 SQL 语句
     *
     * @param sql 要预编译的 SQL 语句
     * @return 新创建的 PreparedStatement 对象
     * @throws SQLException 如果创建过程中发生错误
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new SsPreparedStatement(this, sql);
    }

    /**
     * 创建 CallableStatement 对象用于执行 SQL 存储过程
     *
     * @param sql 要执行的 SQL 语句
     * @return 新创建的 CallableStatement 对象
     * @throws SQLException 如果创建过程中发生错误
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new SsCallableStatement(this, sql);
    }

    /**
     * 将给定的 SQL 语句转换为数据库系统的本地 SQL 语法
     * SmallSQL 不需要转换，直接返回原 SQL
     *
     * @param sql 标准 SQL 语句
     * @return 数据库系统特定的 SQL 语句
     */
    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    /**
     * 设置连接的自动提交模式
     *
     * @param autoCommit true 表示启用自动提交，false 表示禁用自动提交
     * @throws SQLException 如果设置过程中发生错误
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (log.isLogging())
            log.println("AutoCommit:" + autoCommit);
        if (this.autoCommit != autoCommit) {
            commit();
            this.autoCommit = autoCommit;
        }
    }

    /**
     * 获取连接的自动提交模式
     *
     * @return 如果启用自动提交返回 true，否则返回 false
     */
    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * 添加一个事务步骤，用于后续的提交或回滚操作
     */
    public void add(TransactionStep storePage) throws SQLException {
        testClosedConnection();
        synchronized (getMonitor()) {
            commitPages.add(storePage);
        }
    }

    /**
     * 提交当前事务
     *
     * @throws SQLException 如果提交过程中发生错误
     */
    @Override
    public void commit() throws SQLException {
        log.println("Commit");
        testClosedConnection();
        synchronized (getMonitor()) {
            try {
                int count = commitPages.size();
                for (int i = 0; i < count; i++) {
                    TransactionStep page = (TransactionStep) commitPages.get(i);
                    page.commit();
                }
                for (int i = 0; i < count; i++) {
                    TransactionStep page = (TransactionStep) commitPages.get(i);
                    page.freeLock();
                }
                commitPages.clear();
                transactionTime = System.currentTimeMillis();
            } catch (Throwable e) {
                rollback();
                throw SmallSQLException.createFromException(e);
            }
        }
    }

    /**
     * 由于文件被删除而丢弃所有更改
     */
    public void rollbackFile(FileChannel raFile) throws SQLException {
        testClosedConnection();
        // remove the all commits that point to this table
        synchronized (getMonitor()) {
            for (int i = commitPages.size() - 1; i >= 0; i--) {
                TransactionStep page = (TransactionStep) commitPages.get(i);
                if (page.raFile == raFile) {
                    page.rollback();
                    page.freeLock();
                }
            }
        }
    }

    /**
     * 回滚到指定的保存点
     *
     * @param savepoint 保存点位置
     * @throws SQLException 如果回滚过程中发生错误
     */
    public void rollback(int savepoint) throws SQLException {
        testClosedConnection();
        synchronized (getMonitor()) {
            for (int i = commitPages.size() - 1; i >= savepoint; i--) {
                TransactionStep page = (TransactionStep) commitPages.remove(i);
                page.rollback();
                page.freeLock();
            }
        }
    }

    /**
     * 回滚当前事务
     *
     * @throws SQLException 如果回滚过程中发生错误
     */
    @Override
    public void rollback() throws SQLException {
        log.println("Rollback");
        testClosedConnection();
        synchronized (getMonitor()) {
            int count = commitPages.size();
            for (int i = 0; i < count; i++) {
                TransactionStep page = (TransactionStep) commitPages.get(i);
                page.rollback();
                page.freeLock();
            }
            commitPages.clear();
            transactionTime = System.currentTimeMillis();
        }
    }

    /**
     * 关闭连接并回滚未提交的事务
     *
     * @throws SQLException 如果关闭过程中发生错误
     */
    @Override
    public void close() throws SQLException {
        rollback();
        database = null;
        commitPages = null;
        Database.closeConnection(this);
    }

    /**
     * 测试连接是否已关闭，例如被其他线程关闭
     *
     * @throws SQLException 如果连接已关闭
     */
    public final void testClosedConnection() throws SQLException {
        if (isClosed())
            throw SmallSQLException.create(Language.CONNECTION_CLOSED);
    }

    /**
     * 检查连接是否已关闭
     *
     * @return 如果连接已关闭返回 true，否则返回 false
     */
    @Override
    public boolean isClosed() {
        return (commitPages == null);
    }

    /**
     * 获取数据库元数据
     *
     * @return DatabaseMetaData 对象
     */
    @Override
    public DatabaseMetaData getMetaData() {
        return metadata;
    }

    /**
     * 设置连接的只读模式
     *
     * @param readOnly true 表示设置为只读模式
     */
    @Override
    public void setReadOnly(boolean readOnly) {
        // TODO Connection ReadOnly implementing
    }

    /**
     * 检查连接是否为只读模式
     *
     * @return 如果连接为只读模式返回 true，否则返回 false
     */
    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * 设置连接的当前目录（数据库）
     *
     * @param catalog 数据库名称
     * @throws SQLException 如果设置过程中发生错误
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        testClosedConnection();
        database = Database.getDatabase(catalog, this, false);
    }

    /**
     * 获取连接的当前目录（数据库）名称
     *
     * @return 当前数据库名称，如果没有连接到数据库则返回空字符串
     */
    @Override
    public String getCatalog() {
        if (database == null)
            return "";
        return database.getName();
    }

    /**
     * 设置事务隔离级别
     *
     * @param level 事务隔离级别
     * @throws SQLException 如果设置的隔离级别不被支持
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (!metadata.supportsTransactionIsolationLevel(level)) {
            throw SmallSQLException.create(Language.ISOLATION_UNKNOWN, String.valueOf(level));
        }
        isolationLevel = level;
    }

    /**
     * 获取当前事务隔离级别
     *
     * @return 当前事务隔离级别
     */
    @Override
    public int getTransactionIsolation() {
        return isolationLevel;
    }

    /**
     * 获取连接的警告信息
     *
     * @return SQLWarning 对象，如果没有警告则返回 null
     */
    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    /**
     * 清除连接的警告信息
     */
    @Override
    public void clearWarnings() {
        // TODO support for Warnings
    }

    /**
     * 创建具有指定结果集类型和并发性的 Statement 对象
     *
     * @param resultSetType 结果集类型
     * @param resultSetConcurrency 结果集并发性
     * @return 新创建的 Statement 对象
     * @throws SQLException 如果创建过程中发生错误
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SsStatement(this, resultSetType, resultSetConcurrency);
    }

    /**
     * 创建具有指定结果集类型和并发性的 PreparedStatement 对象
     *
     * @param sql SQL 语句
     * @param resultSetType 结果集类型
     * @param resultSetConcurrency 结果集并发性
     * @return 新创建的 PreparedStatement 对象
     * @throws SQLException 如果创建过程中发生错误
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new SsPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    /**
     * 创建具有指定结果集类型和并发性的 CallableStatement 对象
     *
     * @param sql SQL 语句
     * @param resultSetType 结果集类型
     * @param resultSetConcurrency 结果集并发性
     * @return 新创建的 CallableStatement 对象
     * @throws SQLException 如果创建过程中发生错误
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SsCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    public Map getTypeMap() {
        return null;
    }

    public void setTypeMap(Map map) {
        // TODO support for TypeMap
    }

    @Override
    public void setHoldability(int holdability) {
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() {
        return holdability;
    }

    public int getSavepoint() throws SQLException {
        testClosedConnection();
        return commitPages.size(); // the call is atomic, that it need not be
        // synchronized
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return new SsSavepoint(getSavepoint(), null, transactionTime);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return new SsSavepoint(getSavepoint(), name, transactionTime);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (savepoint instanceof SsSavepoint) {
            if (((SsSavepoint) savepoint).transactionTime != transactionTime) {
                throw SmallSQLException.create(Language.SAVEPT_INVALID_TRANS);
            }
            rollback(savepoint.getSavepointId());
            return;
        }
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (savepoint instanceof SsSavepoint) {
            ((SsSavepoint) savepoint).transactionTime = 0;
            return;
        }
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, new Object[]{savepoint});
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        // TODO resultSetHoldability
        return new SsStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        // TODO resultSetHoldability
        return new SsPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        // TODO resultSetHoldability
        return new SsCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        SsPreparedStatement pr = new SsPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(autoGeneratedKeys);
        return pr;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        SsPreparedStatement pr = new SsPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(columnIndexes);
        return pr;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        SsPreparedStatement pr = new SsPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(columnNames);
        return pr;
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
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }


    public void setSchema(String schema) throws SQLException {
        // TODO Auto-generated method stub

    }


    public String getSchema() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }


    public void abort(Executor executor) throws SQLException {
        // TODO Auto-generated method stub

    }


    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO Auto-generated method stub

    }


    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
}