package io.leavesfly.smallsql.rdb.sql.expression.function;

import io.leavesfly.smallsql.rdb.sql.expression.Expression;

import java.sql.SQLException;

/**
 * 函数工厂接口
 * 使用工厂模式解耦函数对象的创建过程
 */
public interface FunctionFactory {
    /**
     * 根据函数类型创建对应的函数表达式对象
     * @param functionType 函数类型
     * @return 对应的函数表达式对象
     */
    Expression createFunction(int functionType) throws SQLException;
}