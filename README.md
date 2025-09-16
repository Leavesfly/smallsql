# SmallSQL 数据库

[![License: LGPL v2.1](https://img.shields.io/badge/License-LGPL%20v2.1-blue.svg)](https://www.gnu.org/licenses/lgpl-2.1)
[![Java Version](https://img.shields.io/badge/Java-1.8+-orange.svg)](https://www.oracle.com/java/)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

SmallSQL 是一个用 Java 编写的轻量级关系数据库管理系统（RDBMS），专为嵌入式应用和学习目的而设计。它提供了完整的 SQL 支持和标准 JDBC 接口，无需任何外部依赖，是一个真正的"零配置"数据库解决方案。

## ✨ 核心特性

### 🎯 核心功能
- **完整的 JDBC 4.0 接口实现** - 标准的 Java 数据库连接接口
- **全面的 SQL 语法支持** - DDL、DML、DQL 语句完整支持
- **事务处理与并发控制** - ACID 属性保证，支持多种隔离级别
- **索引与外键约束** - B+ 树索引实现，完整的约束支持
- **视图（Views）支持** - 虚拟表功能完整实现
- **零依赖架构** - 无需任何外部库，开箱即用

### 🏗️ 技术特色
- **纯 Java 实现** - 跨平台兼容，易于集成
- **文件存储引擎** - 基于文件的持久化存储
- **页式存储管理** - 高效的内存和磁盘管理
- **多语言支持** - 内置国际化框架
- **轻量级设计** - 适合嵌入式和资源受限环境

## 🚀 快速开始

### 环境要求

- **Java**: 1.8 或更高版本
- **Maven**: 3.x（用于构建）
- **操作系统**: 支持 Windows、Linux、macOS

### Maven 依赖

在您的 `pom.xml` 文件中添加依赖：

```xml
<dependency>
    <groupId>org.yf</groupId>
    <artifactId>smallsql</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 基本使用示例

```java
import java.sql.*;

public class SmallSQLExample {
    public static void main(String[] args) throws Exception {
        // 1. 加载驱动（可选，驱动会自动注册）
        Class.forName("io.leavesfly.smallsql.SsDriver");
        
        // 2. 建立连接
        String url = "jdbc:smallsql:./mydb";
        try (Connection conn = DriverManager.getConnection(url)) {
            
            // 3. 创建表
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE users (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name VARCHAR(50), " +
                    "email VARCHAR(100)" +
                    ")");
            }
            
            // 4. 插入数据
            try (PreparedStatement pstmt = conn.prepareStatement(
                "INSERT INTO users (id, name, email) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "张三");
                pstmt.setString(3, "zhangsan@example.com");
                pstmt.executeUpdate();
            }
            
            // 5. 查询数据
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {
                while (rs.next()) {
                    System.out.printf("ID: %d, 姓名: %s, 邮箱: %s%n",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("email"));
                }
            }
        }
    }
}
```

## 🏛️ 系统架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    应用程序接口层                          │
├─────────────────────────────────────────────────────────────┤
│            JDBC 接口层 (SsDriver, SsConnection)            │
├─────────────────────────────────────────────────────────────┤
│                      命令处理层                             │
│  ┌─────────────┬───────────────┬─────────────────────┐    │
│  │ DDL 命令    │   DML 命令      │      DQL 命令       │    │
│  │ (CREATE,    │ (INSERT,        │     (SELECT)        │    │
│  │  DROP,      │  UPDATE,        │                     │    │
│  │  ALTER)     │  DELETE)        │                     │    │
│  └─────────────┴───────────────┴─────────────────────┘    │
├─────────────────────────────────────────────────────────────┤
│                     存储引擎层                              │
│  ┌───────────┬──────────┬───────────┬─────────────────┐     │
│  │ Database  │  Table   │   Index   │ Transaction     │     │
│  │ Manager   │ Manager  │  Manager  │ Manager         │     │
│  └───────────┴──────────┴───────────┴─────────────────┘     │
├─────────────────────────────────────────────────────────────┤
│                     文件存储层                              │
│             (.tbl, .idx, .lob, .master)                    │
└─────────────────────────────────────────────────────────────┘
```

### 分层架构详解

#### 🔌 JDBC 接口层
- **职责**: 提供标准 JDBC 接口实现
- **核心类**: [`SsDriver`](src/main/java/io/leavesfly/smallsql/SsDriver.java), [`SsConnection`](src/main/java/io/leavesfly/smallsql/jdbc/SsConnection.java), [`SsStatement`](src/main/java/io/leavesfly/smallsql/jdbc/statement/SsStatement.java), [`SsResultSet`](src/main/java/io/leavesfly/smallsql/jdbc/SsResultSet.java)
- **特性**: URL 解析、连接管理、事务控制、元数据访问

#### ⚡ 命令处理层
- **职责**: SQL 命令解析与执行
- **设计模式**: 命令模式（Command Pattern）
- **命令类型**:
  - **DDL**: [`CommandCreateDatabase`](src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandCreateDatabase.java), [`CommandTable`](src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandTable.java), [`CommandDrop`](src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandDrop.java)
  - **DML**: [`CommandInsert`](src/main/java/io/leavesfly/smallsql/rdb/command/dml/CommandInsert.java), [`CommandUpdate`](src/main/java/io/leavesfly/smallsql/rdb/command/dml/CommandUpdate.java), [`CommandDelete`](src/main/java/io/leavesfly/smallsql/rdb/command/dml/CommandDelete.java)
  - **DQL**: [`CommandSelect`](src/main/java/io/leavesfly/smallsql/rdb/command/dql/CommandSelect.java)

#### 💾 存储引擎层
- **职责**: 数据持久化与管理
- **核心组件**:
  - **[`Database`](src/main/java/io/leavesfly/smallsql/rdb/engine/Database.java)**: 数据库实例管理
  - **[`Table`](src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java)**: 表数据管理与 CRUD 操作
  - **[`Index`](src/main/java/io/leavesfly/smallsql/rdb/engine/Index.java)**: B+ 树索引实现
  - **[`View`](src/main/java/io/leavesfly/smallsql/rdb/engine/View.java)**: 视图管理

## 📊 SQL 语法支持

### DDL（数据定义语言）

```sql
-- 创建数据库
CREATE DATABASE mydb;

-- 创建表
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INTEGER DEFAULT 0
);

-- 创建索引
CREATE INDEX idx_user_email ON users(email);

-- 创建视图
CREATE VIEW adult_users AS 
SELECT * FROM users WHERE age >= 18;
```

### DML（数据操作语言）

```sql
-- 插入数据
INSERT INTO users (id, name, email, age) 
VALUES (1, '张三', 'zhangsan@example.com', 25);

-- 更新数据
UPDATE users 
SET age = 26, email = 'zhangsan_new@example.com' 
WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE age < 18;
```

### DQL（数据查询语言）

```sql
-- 基本查询
SELECT * FROM users;
SELECT name, email FROM users WHERE age > 25;

-- 聚合查询
SELECT COUNT(*) as total_users, AVG(age) as avg_age
FROM users;

-- 分组查询
SELECT age, COUNT(*) as count 
FROM users 
GROUP BY age 
HAVING COUNT(*) > 1;

-- 连接查询
SELECT u.name, p.title 
FROM users u 
INNER JOIN posts p ON u.id = p.user_id;
```

## 🔒 事务与并发控制

### 事务管理

```java
// 手动事务控制
Connection conn = DriverManager.getConnection("jdbc:smallsql:./mydb");
conn.setAutoCommit(false);

try {
    // 执行多个操作
    PreparedStatement pstmt1 = conn.prepareStatement("INSERT INTO users (name) VALUES (?)");
    pstmt1.setString(1, "用户1");
    pstmt1.executeUpdate();
    
    PreparedStatement pstmt2 = conn.prepareStatement("UPDATE accounts SET balance = balance - 100 WHERE id = ?");
    pstmt2.setInt(1, 1);
    pstmt2.executeUpdate();
    
    // 提交事务
    conn.commit();
} catch (SQLException e) {
    // 回滚事务
    conn.rollback();
    throw e;
} finally {
    conn.setAutoCommit(true);
    conn.close();
}
```

### 并发控制特性

- **表级锁**: 支持共享锁和排他锁
- **行级锁**: 细粒度的数据访问控制
- **页级锁**: 存储页面的并发控制
- **死锁检测**: 自动检测和处理死锁情况
- **隔离级别**: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ

## 📂 文件存储格式

### 文件组织结构

```
mydb/                          # 数据库目录
├── database.master           # 数据库主配置文件
├── users.tbl                 # 用户表数据文件
├── users.idx                 # 用户表索引文件
├── users.lob                 # 大对象存储文件
├── posts.tbl                 # 文章表数据文件
├── posts.idx                 # 文章表索引文件
└── transaction.log           # 事务日志文件
```

### 存储特性

- **页式存储**: 以页为单位管理数据，支持高效缓存
- **索引分离**: 索引文件独立存储，优化查询性能
- **LOB 支持**: 大对象（BLOB/CLOB）外部存储
- **事务日志**: 支持事务回滚和恢复
- **文件锁定**: 防止多进程同时访问同一数据库

## 🛠️ 构建与开发

### 从源码构建

```bash
# 克隆项目
git clone <repository-url>
cd smallsql

# 编译项目
mvn clean compile

# 运行测试
mvn test

# 生成 JAR 包
mvn package

# 生成的 JAR 文件位于 target/smallsql-1.0.0-SNAPSHOT.jar
```

### 运行测试

```bash
# 运行所有测试
mvn test

# 运行特定测试类
mvn test -Dtest=TestCreateDropDB

# 运行特定测试方法
mvn test -Dtest=TestCreateDropDB#testCreateDatabase
```

## 📁 项目结构

```
src/main/java/io/leavesfly/smallsql/
├── SsDriver.java                    # JDBC 驱动程序入口
├── jdbc/                           # JDBC 接口实现层
│   ├── SsConnection.java           # 数据库连接实现
│   ├── SsResultSet.java           # 结果集实现
│   ├── statement/                  # 语句实现
│   │   ├── SsStatement.java
│   │   ├── SsPreparedStatement.java
│   │   └── SsCallableStatement.java
│   ├── metadata/                   # 元数据支持
│   │   ├── SsDatabaseMetaData.java
│   │   └── SsResultSetMetaData.java
│   └── SmallSQLException.java      # 异常处理
├── rdb/                           # 关系数据库核心
│   ├── command/                   # 命令处理层
│   │   ├── Command.java           # 命令基类
│   │   ├── ddl/                   # 数据定义语言命令
│   │   ├── dml/                   # 数据操作语言命令
│   │   └── dql/                   # 数据查询语言命令
│   ├── engine/                    # 存储引擎层
│   │   ├── Database.java          # 数据库管理
│   │   ├── Table.java             # 表管理
│   │   ├── Index.java             # 索引管理
│   │   ├── View.java              # 视图管理
│   │   └── store/                 # 存储管理
│   └── sql/                       # SQL 解析和处理
│       ├── SQLParser.java         # SQL 解析器
│       ├── datatype/              # 数据类型系统
│       ├── expression/            # 表达式处理
│       └── parser/                # 解析器组件
├── lang/                          # 国际化支持
├── logger/                        # 日志系统
└── util/                          # 工具类
```

## 📈 性能特性

### 优化策略
- **B+ 树索引**: 加速查询性能
- **页面缓存**: 减少磁盘 I/O
- **延迟写入**: 批量写入优化
- **连接池**: 支持连接复用

### 适用场景
- **嵌入式应用**: 无需额外的数据库服务器
- **开发测试**: 快速搭建开发环境
- **教学学习**: 理解数据库原理的最佳工具
- **小型项目**: 中小型数据集的理想选择

## 🤝 贡献指南

我们欢迎社区贡献！请遵循以下步骤：

1. **Fork 项目**到您的 GitHub 账户
2. **创建特性分支** (`git checkout -b feature/AmazingFeature`)
3. **提交更改** (`git commit -m 'Add some AmazingFeature'`)
4. **推送分支** (`git push origin feature/AmazingFeature`)
5. **创建 Pull Request**

### 代码规范
- 遵循 Java 标准编码规范
- 添加适当的单元测试
- 更新相关文档
- 确保所有测试通过

## 📄 许可证

本项目采用 **GNU Lesser General Public License v2.1** 许可证。

这意味着您可以：
- ✅ 在商业和非商业项目中使用
- ✅ 修改源代码
- ✅ 分发原始或修改后的代码
- ✅ 私人使用

但需要：
- 📋 保留原始许可证和版权声明
- 📋 说明您所做的更改（如果有）
- 📋 在修改后的版本中包含相同的许可证

## ⚠️ 注意事项

- **适用范围**: 适合小到中型应用，不建议用于大规模生产环境
- **测试建议**: 建议在使用前进行充分的测试
- **版本兼容性**: 数据文件格式可能在版本间发生变化，升级时请注意兼容性
- **备份策略**: 定期备份数据库文件，确保数据安全

## 📞 获取帮助

如果您遇到问题或有疑问，可以通过以下方式获取帮助：

- 📖 查看[项目文档](docs/)
- 💬 创建 [GitHub Issue](../../issues)
- 🔍 搜索现有的问题和解决方案
- 📧 联系维护者

---

**SmallSQL** - 让数据库开发变得简单而高效！ 🚀

## 数据存储格式

### 文件组织结构:
```
database_directory/
├── database.master     # 数据库主文件
├── table1.tbl         # 表数据文件
├── table1.idx         # 表索引文件
├── table1.lob         # 大对象文件
└── ...
```

### 存储特性:
- 每个表对应独立的数据文件
- 索引文件单独存储
- 支持大对象(BLOB/CLOB)的外部存储
- 页式存储，支持缓存优化

## 并发控制

### 锁定机制:
- **表级锁**: 支持共享锁和排他锁
- **行级锁**: 细粒度的数据访问控制
- **页级锁**: 存储页面的并发控制

### 事务支持:
- ACID属性保证
- 自动提交和手动事务控制
- 死锁检测和处理
- 事务回滚机制

## 性能特性

### 优化策略:
- B+树索引加速查询
- 页面缓存机制
- 延迟写入优化
- 批量操作支持

### 适用场景:
- 嵌入式应用
- 小到中型数据集
- 开发和测试环境
- 教学和学习用途

## 构建和使用

### 构建要求:
- Java 1.6+
- Maven 3.x

### 构建命令:
```bash
mvn clean compile
mvn test
mvn package
```

### JDBC连接示例:
```java
// 加载驱动
Class.forName("io.leavesfly.smallsql.SsDriver");

// 建立连接
String url = "jdbc:smallsql:./mydb";
Connection conn = DriverManager.getConnection(url);

// 执行SQL
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM users");

// 处理结果
while(rs.next()) {
    System.out.println(rs.getString("name"));
}

// 关闭连接
rs.close();
stmt.close();
conn.close();
```

## 项目结构

```
src/main/java/io/leavesfly/smallsql/
├── SsDriver.java              # JDBC驱动入口
├── jdbc/                      # JDBC接口实现
│   ├── SsConnection.java
│   ├── SsStatement.java
│   ├── SsResultSet.java
│   └── metadata/             # 元数据支持
├── rdb/                      # 关系数据库核心
│   ├── command/              # SQL命令处理
│   │   ├── ddl/             # 数据定义语言
│   │   ├── dml/             # 数据操作语言
│   │   └── dql/             # 数据查询语言
│   ├── engine/              # 存储引擎
│   │   ├── Database.java
│   │   ├── Table.java
│   │   ├── Index.java
│   │   └── store/           # 存储管理
│   └── sql/                 # SQL解析和处理
├── lang/                    # 国际化支持
├── logger/                  # 日志系统
└── util/                    # 工具类
```

## 贡献和开发

这是一个开源项目，欢迎贡献代码和改进建议。项目采用LGPL v2.1许可证，允许在商业和非商业项目中使用。

## 注意事项

- 该数据库适合小到中型应用，不建议用于大规模生产环境
- 建议在使用前进行充分的测试
- 数据文件格式可能在版本间发生变化，升级时请注意兼容性