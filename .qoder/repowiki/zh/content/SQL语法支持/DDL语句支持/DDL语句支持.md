# DDL语句支持

<cite>
**本文档中引用的文件**  
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java)
- [CommandCreateDatabase.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandCreateDatabase.java)
- [CommandTable.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandTable.java)
- [CommandDrop.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandDrop.java)
- [CommandCreateView.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandCreateView.java)
- [Column.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Column.java)
- [Columns.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Columns.java)
- [ForeignKey.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/ForeignKey.java)
</cite>

## 目录
1. [简介](#简介)
2. [DDL语句语法结构](#ddl语句语法结构)
3. [表结构定义](#表结构定义)
4. [SQLParser解析机制](#sqlparser解析机制)
5. [命令执行实现](#命令执行实现)
6. [实际SQL示例](#实际sql示例)

## 简介
SmallSQL支持完整的数据定义语言（DDL）功能，允许用户创建和管理数据库对象。本文档详细说明了CREATE DATABASE、CREATE TABLE、CREATE VIEW、DROP和ALTER TABLE等DDL语句的语法结构、使用方式和内部实现机制。通过分析SQLParser的解析流程和相关命令类的执行逻辑，全面展示SmallSQL如何处理各种DDL操作。

## DDL语句语法结构

### CREATE语句
SmallSQL的CREATE语句支持创建数据库、表、视图、索引和存储过程等对象。SQLParser通过create()方法根据不同的关键字分发到相应的实现类。

```mermaid
flowchart TD
Start([CREATE语句]) --> CheckKeyword{检查关键字}
CheckKeyword --> |DATABASE| CreateDatabase[CommandCreateDatabase]
CheckKeyword --> |TABLE| CreateTable[CommandTable]
CheckKeyword --> |VIEW| CreateView[CommandCreateView]
CheckKeyword --> |INDEX| CreateIndex[CommandTable]
CheckKeyword --> |PROCEDURE| CreateProcedure[CommandCreateDatabase]
```

**Diagram sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1000-L1020)

**Section sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1000-L1050)

### DROP语句
DROP语句用于删除数据库对象，支持删除数据库、表、视图、索引和存储过程。

```mermaid
flowchart TD
Start([DROP语句]) --> CheckType{检查对象类型}
CheckType --> |DATABASE| DropDatabase[删除数据库]
CheckType --> |TABLE| DropTable[删除表]
CheckType --> |VIEW| DropView[删除视图]
CheckType --> |INDEX| DropIndex[删除索引]
CheckType --> |PROCEDURE| DropProcedure[删除存储过程]
```

**Diagram sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1500-L1530)

**Section sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1500-L1550)
- [CommandDrop.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandDrop.java#L20-L30)

### ALTER语句
ALTER语句用于修改现有数据库对象，目前主要支持ALTER TABLE操作。

```mermaid
flowchart TD
Start([ALTER语句]) --> CheckType{检查对象类型}
CheckType --> |TABLE| AlterTable[ALTER TABLE]
CheckType --> |VIEW| AlterView[ALTER VIEW]
CheckType --> |INDEX| AlterIndex[ALTER INDEX]
CheckType --> |PROCEDURE| AlterProcedure[ALTER PROCEDURE]
AlterTable --> CheckOperation{检查操作类型}
CheckOperation --> |ADD| AddColumn[添加列]
CheckOperation --> |ALTER| ModifyColumn[修改列]
CheckOperation --> |DROP| DropColumn[删除列]
```

**Diagram sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1550-L1600)

**Section sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1550-L1650)

## 表结构定义

### 列类型定义
SmallSQL支持多种数据类型，包括数值型、字符型、日期时间型等。Column类用于表示表中的列，包含数据类型、精度、标度等属性。

```mermaid
classDiagram
class Column {
+String name
+int dataType
+int precision
+int scale
+boolean nullable
+boolean identity
+Expression defaultValue
+String defaultDefinition
+setName(String)
+setDataType(int)
+setPrecision(int)
+setScale(int)
+setNullable(boolean)
+setAutoIncrement(boolean)
+setDefaultValue(Expression, String)
}
class Columns {
+int size
+Column[] data
+add(Column)
+get(int)
+get(String)
+size()
+copy()
}
Columns "1" *-- "0..*" Column : 包含
```

**Diagram sources**
- [Column.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Column.java#L30-L190)
- [Columns.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Columns.java#L20-L140)

**Section sources**
- [Column.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Column.java#L30-L190)
- [Columns.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Columns.java#L20-L140)

### 约束定义
表结构支持主键、唯一约束、外键等约束定义。IndexDescription类用于表示索引和约束，ForeignKey类用于表示外键关系。

```mermaid
classDiagram
class IndexDescription {
+String name
+String tableName
+int constraintType
+Expressions expressions
+Strings columns
+IndexDescription(String, String, int, Expressions, Strings)
}
class IndexDescriptions {
+add(IndexDescription)
+get(int)
+size()
}
class ForeignKey {
+String pkTable
+String fkTable
+IndexDescription pk
+IndexDescription fk
+int updateRule
+int deleteRule
+ForeignKey(String, IndexDescription, String, IndexDescription)
}
class ForeignKeys {
+add(ForeignKey)
+get(int)
+size()
}
IndexDescriptions "1" *-- "0..*" IndexDescription : 包含
ForeignKeys "1" *-- "0..*" ForeignKey : 包含
CommandTable --> IndexDescriptions : 使用
CommandTable --> ForeignKeys : 使用
```

**Diagram sources**
- [IndexDescription.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/index/IndexDescription.java)
- [ForeignKey.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/ForeignKey.java#L20-L55)

**Section sources**
- [IndexDescription.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/index/IndexDescription.java)
- [ForeignKey.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/ForeignKey.java#L20-L55)

## SQLParser解析机制

### create()方法分发机制
SQLParser的create()方法是DDL语句解析的核心，根据不同的关键字分发到相应的创建方法。

```mermaid
sequenceDiagram
participant Parser as SQLParser
participant Token as SQLToken
participant CreateDB as createDatabase()
participant CreateTable as createTable()
participant CreateView as createView()
participant CreateIndex as createIndex()
Parser->>Parser : create()
Parser->>Token : nextToken(COMMANDS_CREATE)
alt DATABASE关键字
Token-->>Parser : DATABASE
Parser->>CreateDB : createDatabase()
CreateDB-->>Parser : CommandCreateDatabase
Parser-->>Parser : 返回CommandCreateDatabase
end
alt TABLE关键字
Token-->>Parser : TABLE
Parser->>CreateTable : createTable()
CreateTable-->>Parser : CommandTable
Parser-->>Parser : 返回CommandTable
end
alt VIEW关键字
Token-->>Parser : VIEW
Parser->>CreateView : createView()
CreateView-->>Parser : CommandCreateView
Parser-->>Parser : 返回CommandCreateView
end
alt INDEX关键字
Token-->>Parser : INDEX
Parser->>CreateIndex : createIndex(false)
CreateIndex-->>Parser : CommandTable
Parser-->>Parser : 返回CommandTable
end
```

**Diagram sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1000-L1050)

**Section sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L1000-L1050)

### 表定义解析流程
CREATE TABLE语句的解析流程包括列定义和约束定义两个主要部分。

```mermaid
flowchart TD
Start([开始解析CREATE TABLE]) --> ReadTableName[读取表名]
ReadTableName --> CheckParenthesis{检查左括号}
CheckParenthesis --> |是| ParseColumns[解析列定义]
ParseColumns --> ParseColumn{解析列}
ParseColumn --> ReadColumnName[读取列名]
ParseColumn --> ParseDataType[解析数据类型]
ParseDataType --> ParseOptions[解析列选项]
ParseOptions --> |DEFAULT| SetDefault[设置默认值]
ParseOptions --> |IDENTITY| SetIdentity[设置自增]
ParseOptions --> |NULL/NOT NULL| SetNullable[设置可空性]
ParseOptions --> |PRIMARY KEY/UNIQUE| AddIndex[添加索引]
ParseColumn --> CheckSeparator{检查分隔符}
CheckSeparator --> |逗号| ParseColumn
CheckSeparator --> |右括号| End[结束解析]
ParseColumns --> ParseConstraint{解析约束}
ParseConstraint --> |CONSTRAINT| ReadConstraintName[读取约束名]
ParseConstraint --> |PRIMARY KEY| AddPrimaryKey[添加主键]
ParseConstraint --> |UNIQUE| AddUnique[添加唯一约束]
ParseConstraint --> |FOREIGN KEY| AddForeignKey[添加外键]
ParseConstraint --> CheckSeparator2{检查分隔符}
CheckSeparator2 --> |逗号| ParseConstraint
CheckSeparator2 --> |右括号| End
```

**Diagram sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L850-L950)

**Section sources**
- [SQLParser.java](file://src/main/java/io/leavesfly/smallsql/rdb/sql/SQLParser.java#L850-L950)

## 命令执行实现

### CommandTable类处理逻辑
CommandTable类负责处理表的创建和修改操作，包括列定义和约束设置。

```mermaid
sequenceDiagram
participant Parser as SQLParser
participant CmdTable as CommandTable
participant Database as Database
participant Table as Table
Parser->>CmdTable : createTable()
CmdTable->>CmdTable : 初始化Columns、IndexDescriptions、ForeignKeys
Parser->>CmdTable : addColumn()
CmdTable->>Columns : add(Column)
Parser->>CmdTable : addIndex()
CmdTable->>IndexDescriptions : add(IndexDescription)
Parser->>CmdTable : addForeingnKey()
CmdTable->>ForeignKeys : add(ForeignKey)
CmdTable->>Database : executeImpl()
alt CREATE操作
Database->>Table : createTable()
Table-->>Database : 新表
Database-->>CmdTable : 完成
end
alt ADD操作
Database->>Table : requestLock()
Database->>Table : createTable(新表名)
Database->>Database : INSERT INTO...SELECT FROM
Database->>Database : replaceTable()
Database-->>CmdTable : 完成
end
```

**Diagram sources**
- [CommandTable.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandTable.java#L50-L150)

**Section sources**
- [CommandTable.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandTable.java#L50-L150)

### CommandDrop类删除功能
CommandDrop类实现了数据库对象的删除功能，根据不同对象类型执行相应的删除操作。

```mermaid
sequenceDiagram
participant Parser as SQLParser
participant CmdDrop as CommandDrop
participant Database as Database
participant File as File
Parser->>CmdDrop : drop()
CmdDrop->>CmdDrop : 创建CommandDrop实例
CmdDrop->>CmdDrop : executeImpl()
alt DATABASE类型
CmdDrop->>File : 检查目录存在
CmdDrop->>File : 删除所有文件
CmdDrop->>File : 删除目录
CmdDrop-->>Parser : 完成
end
alt TABLE类型
CmdDrop->>Database : dropTable()
Database-->>CmdDrop : 完成
end
alt VIEW类型
CmdDrop->>Database : dropView()
Database-->>CmdDrop : 完成
end
alt INDEX/PROCEDURE类型
CmdDrop->>CmdDrop : 抛出UnsupportedOperationException
CmdDrop-->>Parser : 异常
end
```

**Diagram sources**
- [CommandDrop.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandDrop.java#L30-L80)

**Section sources**
- [CommandDrop.java](file://src/main/java/io/leavesfly/smallsql/rdb/command/ddl/CommandDrop.java#L30-L80)

## 实际SQL示例

### 创建数据库
```sql
CREATE DATABASE mydb;
```

### 创建带约束的表
```sql
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    salary DECIMAL(10,2) DEFAULT 0.00,
    department_id INTEGER,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);
```

### 创建视图
```sql
CREATE VIEW employee_summary AS 
SELECT id, name, salary, department_id 
FROM employees 
WHERE salary > 5000;
```

### 删除数据库对象
```sql
DROP TABLE employees;
DROP VIEW employee_summary;
DROP DATABASE mydb;
```

### 修改表结构
```sql
ALTER TABLE employees ADD COLUMN phone VARCHAR(20);
```

**Section sources**
- [TestCreateDropDB.java](file://src/test/java/io/leavesfly/smallsql/junit/sql/ddl/TestCreateDropDB.java)
- [TestAlterTable.java](file://src/test/java/io/leavesfly/smallsql/junit/sql/ddl/TestAlterTable.java)