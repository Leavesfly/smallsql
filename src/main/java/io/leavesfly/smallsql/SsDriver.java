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
 * SSDriver.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql;

import java.sql.*;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.lang.Language;

/**
 * SmallSQL 数据库的 JDBC 驱动程序实现类。
 * <p>
 * 这个类实现了 java.sql.Driver 接口，负责解析 JDBC URL 并创建数据库连接。
 * 它是 SmallSQL 数据库与应用程序之间的入口点。
 * <p>
 * JDBC URL 格式: jdbc:smallsql:[database_path][?property1=value1[&property2=value2]...]
 * 例如: jdbc:smallsql:./mydb?autocommit=true
 */
public class SsDriver implements Driver {

    /**
     * JDBC URL 前缀，用于标识 SmallSQL 数据库连接
     */
    public static final String URL_PREFIX = "jdbc:smallsql";

    /**
     * 驱动程序的单例实例
     */
    public static SsDriver DRIVER;

    /**
     * 静态初始化块，在类加载时注册驱动程序到 DriverManager
     */
    static {
        try {
            DRIVER = new SsDriver();
            DriverManager.registerDriver(DRIVER);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * 建立数据库连接
     * 
     * @param url  JDBC URL，格式为 jdbc:smallsql:[database_path][?property=value...]
     * @param info 连接属性信息
     * @return 成功时返回 SsConnection 连接对象，失败时返回 null
     * @throws SQLException 如果连接过程中发生错误
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        return new SsConnection(parse(url, info));
    }

    /**
     * 解析 JDBC URL 并构建连接属性
     * <p>
     * URL 格式: jdbc:smallsql:[database_path][?property1=value1[&property2=value2]...]
     * 例如: jdbc:smallsql:./mydb?autocommit=true
     *
     * @param url  JDBC URL
     * @param info 连接属性列表
     * @return 包含解析后属性的 Properties 对象
     * @throws SQLException 如果 URL 解析过程中发生错误
     */
    private Properties parse(String url, Properties info) throws SQLException {
        Properties props = (Properties) info.clone();
        if (!acceptsURL(url)) {
            return props;
        }
        int idx1 = url.indexOf(':', 5); // search after "jdbc:"
        int idx2 = url.indexOf('?');
        if (idx1 > 0) {
            String dbPath = (idx2 > 0) ? url.substring(idx1 + 1, idx2) : url.substring(idx1 + 1);
            props.setProperty("dbpath", dbPath);
        }
        if (idx2 > 0) {
            String propsString = url.substring(idx2 + 1).replace('&', ';');
            StringTokenizer tok = new StringTokenizer(propsString, ";");
            while (tok.hasMoreTokens()) {
                String keyValue = tok.nextToken().trim();
                if (keyValue.length() > 0) {
                    idx1 = keyValue.indexOf('=');
                    if (idx1 > 0) {
                        String key = keyValue.substring(0, idx1).toLowerCase().trim();
                        String value = keyValue.substring(idx1 + 1).trim();
                        props.put(key, value);
                    } else {
                        throw SmallSQLException
                                .create(Language.CUSTOM_MESSAGE, "Missing equal in property:" + keyValue);
                    }
                }
            }
        }
        return props;
    }

    /**
     * 检查驱动程序是否可以处理指定的 URL
     * 
     * @param url 要检查的 JDBC URL
     * @return 如果 URL 可以被此驱动程序处理则返回 true，否则返回 false
     */
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(URL_PREFIX);
    }

    /**
     * 获取驱动程序的属性信息
     * 
     * @param url  JDBC URL
     * @param info 连接属性
     * @return 驱动程序属性信息数组
     * @throws SQLException 如果获取属性信息过程中发生错误
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties props = parse(url, info);
        DriverPropertyInfo[] driverInfos = new DriverPropertyInfo[1];
        driverInfos[0] = new DriverPropertyInfo("dbpath", props.getProperty("dbpath"));
        return driverInfos;
    }

    /**
     * 获取驱动程序的主版本号
     * 
     * @return 主版本号
     */
    @Override
    public int getMajorVersion() {
        return 0;
    }

    /**
     * 获取驱动程序的次版本号
     * 
     * @return 次版本号
     */
    @Override
    public int getMinorVersion() {
        return 21;
    }

    /**
     * 检查驱动程序是否符合 JDBC 规范
     * 
     * @return 如果符合 JDBC 规范返回 true，否则返回 false
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * 获取父级日志记录器
     * 
     * @return 日志记录器实例
     * @throws SQLFeatureNotSupportedException 如果不支持该功能
     */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO
        return null;
    }
}