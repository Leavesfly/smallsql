# Table结构

<cite>
**本文档中引用的文件**  
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java)
- [View.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/View.java)
- [IndexDescription.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/index/IndexDescription.java)
- [Columns.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/table/Columns.java)
- [Store.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Store.java)
</cite>

## 目录
1. [Table继承View的设计原理](#table继承view的设计原理)
2. [Table数据文件管理机制](#table数据文件管理机制)
3. [索引元数据关联机制](#索引元数据关联机制)
4. [表结构序列化实现](#表结构序列化实现)
5. [多粒度锁机制](#多粒度锁机制)
6. [数据存储访问器获取](#数据存储访问器获取)
7. [代码示例](#代码示例)

## Table继承View的设计原理

`Table`类继承自`View`抽象基类，实现了数据库表的核心功能。`View`类定义了表和视图的公共接口和基础属性，包括名称、列定义和时间戳等。`Table`类通过继承`View`获得了这些基础功能，并在此基础上扩展了表特有的功能，如数据文件管理、索引管理和锁机制。

`Table`类在构造函数中调用`super(name, new Columns())`初始化父类，然后设置数据库引用、文件通道和数据起始偏移量。这种设计模式实现了代码复用，同时保持了表和视图之间的统一接口。

**Section sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)
- [View.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/View.java#L51-L204)

## Table数据文件管理机制

`Table`类通过`raFile`成员变量管理表数据文件，`firstPage`指向数据起始偏移量。`raFile`是`FileChannel`类型，用于随机访问表文件。`firstPage`记录了第一个数据页的文件偏移量，通常在表头信息之后。

当从现有表加载数据时，构造函数首先读取表头信息，然后使用`getStore`方法获取数据存储访问器。`write`方法负责将表结构序列化到文件，包括列定义和索引描述。`createFile`方法创建新的表文件并写入魔数和版本信息。

```mermaid
classDiagram
class View {
+String name
+Columns columns
+long timestamp
+File getFile(Database)
+FileChannel createFile(SsConnection, Database)
}
class Table {
+Database database
+FileChannel raFile
+long firstPage
+HashMap~Long, TableStorePage~ locks
+ArrayList~TableStorePage~ locksInsert
+IndexDescriptions indexes
+ForeignKeys references
-void write(SsConnection)
+StoreImpl getStore(SsConnection, long, int)
+StoreImpl getStoreInsert(SsConnection)
+StorePageLink[] getInserts(SsConnection)
+TableStorePage requestLock(SsConnection, int, long)
}
View <|-- Table
```

**Diagram sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)
- [View.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/View.java#L51-L204)

**Section sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)
- [View.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/View.java#L51-L204)

## 索引元数据关联机制

`Table`类通过`indexes`成员变量关联索引元数据。`indexes`是`IndexDescriptions`类型，包含表中所有索引的描述信息。每个索引由`IndexDescription`类表示，包含索引名称、约束类型、列列表和表达式等信息。

在表加载过程中，构造函数读取附加信息部分，当遇到`INDEX`类型时，调用`IndexDescription.load`方法加载索引描述。`write`方法在写入表结构时，遍历`indexes`中的每个`IndexDescription`，将其序列化到文件中。

```mermaid
classDiagram
class IndexDescription {
+String name
+int constraintType
+Strings columns
+Expressions expressions
+Index index
+FileChannel raFile
+void create(SsConnection, Database, View)
+void load(Database)
+void save(StoreImpl)
+static IndexDescription load(Database, View, StoreImpl)
}
class IndexDescriptions {
+ArrayList~IndexDescription~ data
+int size()
+IndexDescription get(int)
+void add(IndexDescription)
+void create(SsConnection, Database, View)
+void drop(Database)
+void close()
}
class Table {
+IndexDescriptions indexes
}
Table --> IndexDescriptions : "包含"
IndexDescriptions --> IndexDescription : "包含"
```

**Diagram sources**
- [IndexDescription.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/index/IndexDescription.java#L67-L292)
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

**Section sources**
- [IndexDescription.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/index/IndexDescription.java#L67-L292)
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

## 表结构序列化实现

`write`方法实现了表结构的序列化。该方法首先创建表文件，然后获取数据存储访问器。序列化过程包括：写入列数量，遍历列定义并写入每个列；遍历索引描述，写入索引类型标识、长度占位符、索引描述本身和实际长度。

```mermaid
flowchart TD
Start([开始写入表结构]) --> CreateFile["创建表文件"]
CreateFile --> SetFirstPage["设置firstPage为8"]
SetFirstPage --> GetStore["获取数据存储访问器"]
GetStore --> WriteColumnCount["写入列数量"]
WriteColumnCount --> WriteColumns["遍历并写入每个列"]
WriteColumns --> WriteIndexes["遍历索引描述"]
WriteIndexes --> WriteIndexType["写入INDEX类型标识"]
WriteIndexType --> WriteLengthPlaceholder["写入长度占位符"]
WriteLengthPlaceholder --> WriteIndexDesc["写入索引描述"]
WriteIndexDesc --> WriteActualLength["写入实际长度"]
WriteActualLength --> CheckMoreIndexes{"还有更多索引?"}
CheckMoreIndexes --> |是| WriteIndexes
CheckMoreIndexes --> |否| WriteEndMarker["写入结束标记0"]
WriteEndMarker --> WriteFinish["调用writeFinsh"]
WriteFinish --> UpdateFirstPage["更新firstPage"]
UpdateFirstPage --> End([结束])
```

**Diagram sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

**Section sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

## 多粒度锁机制

`Table`类实现了多粒度锁机制，支持行锁、页锁和表锁。锁机制通过`locks`、`locksInsert`和`tabLockConnection`等成员变量实现。`requestLock`方法是锁请求的入口，根据操作类型和隔离级别决定是否授予锁。

对于`INSERT`操作，创建`TableStorePageInsert`锁并添加到`locksInsert`列表；对于`SELECT`和`UPDATE`操作，检查现有锁的兼容性，创建相应的读锁或写锁；对于`CREATE`和`ALTER`操作，需要获取表级锁。

```mermaid
sequenceDiagram
participant Client as "客户端"
participant Table as "Table"
participant Locks as "锁管理器"
Client->>Table : requestLock(con, pageOperation, page)
Table->>Table : 检查raFile是否为空
Table->>Locks : 同步锁管理器
alt 表锁被其他连接持有
Locks-->>Table : 返回null
Table-->>Client : 抛出异常
else
Locks->>Locks : 根据操作类型处理
alt CREATE/ALTER操作
Locks->>Locks : 检查是否有其他锁
Locks->>Locks : 设置tabLockConnection
Locks->>Locks : 创建TableStorePage锁
Locks-->>Table : 返回锁对象
else INSERT操作
Locks->>Locks : 检查序列化连接
Locks->>Locks : 创建TableStorePageInsert锁
Locks->>Locks : 添加到locksInsert
Locks-->>Table : 返回锁对象
else SELECT/UPDATE操作
Locks->>Locks : 获取页锁
alt 存在兼容锁
Locks-->>Table : 返回可用锁
else
Locks->>Locks : 创建新锁
Locks->>Locks : 根据操作类型设置锁类型
Locks-->>Table : 返回新锁
end
end
Table-->>Client : 返回锁对象
end
```

**Diagram sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

**Section sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

## 数据存储访问器获取

`Table`类提供了`getStore`系列方法来获取不同操作类型的数据存储访问器。`getStore`方法用于读取和更新数据页，`getStoreInsert`方法用于获取插入操作的存储访问器，`getLobStore`方法用于获取LOB数据的存储访问器。

```mermaid
classDiagram
class Store {
+static Store NULL
+static Store NOROW
+boolean isNull(int, int)
+boolean getBoolean(int, int)
+byte[] getBytes(int, int)
+double getDouble(int, int)
+float getFloat(int, int)
+int getInt(int, int)
+long getLong(int, int)
+long getMoney(int, int)
+MutableNumeric getNumeric(int, int)
+Object getObject(int, int)
+String getString(int, int)
+boolean isValidPage()
+void scanObjectOffsets(int[], int[])
+int getUsedSize()
+long getNextPagePos()
+void deleteRow(SsConnection)
}
class StoreImpl {
+Table table
+StorePage storePage
+int pageOperation
+long filePos
+void writeInt(int)
+void writeString(String)
+void writeColumn(Column)
+void writeFinsh(SsConnection)
+int readInt()
+String readString()
+Column readColumn(int)
+long getNextPagePos()
+int getCurrentOffsetInPage()
+void setCurrentOffsetInPage(int)
}
class Table {
+StoreImpl getStore(SsConnection, long, int)
+StoreImpl getStoreInsert(SsConnection)
+StoreImpl getLobStore(SsConnection, long, int)
+StoreImpl getStoreTemp(SsConnection)
}
Store <|-- StoreImpl
Table --> StoreImpl : "创建"
```

**Diagram sources**
- [Store.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Store.java#L46-L90)
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

**Section sources**
- [Store.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Store.java#L46-L90)
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

## 代码示例

以下代码示例展示了如何通过`Table.getStore()`读取数据页和`Table.getInserts()`获取未提交的插入记录：

```mermaid
sequenceDiagram
participant App as "应用程序"
participant Table as "Table"
participant Store as "StoreImpl"
App->>Table : getStore(con, firstPage, SELECT)
Table->>Table : requestLock(con, SELECT, firstPage)
Table->>Table : 创建TableStorePage锁
Table->>Store : createStore(table, storePage, SELECT, firstPage)
Store-->>Table : 返回StoreImpl对象
Table-->>App : 返回StoreImpl对象
App->>Store : 读取数据
Store-->>App : 返回数据
App->>Table : getInserts(con)
Table->>Table : 同步锁管理器
Table->>Table : 检查隔离级别
alt READ_UNCOMMITTED
Table->>Table : 返回所有未提交插入
else
Table->>Table : 返回当前连接的未提交插入
end
Table-->>App : 返回StorePageLink列表
```

**Diagram sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)

**Section sources**
- [Table.java](file://src/main/java/io/leavesfly/smallsql/rdb/engine/Table.java#L59-L607)