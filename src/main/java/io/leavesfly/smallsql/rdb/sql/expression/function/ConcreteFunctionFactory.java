package io.leavesfly.smallsql.rdb.sql.expression.function;

import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.sql.expression.Expression;
import io.leavesfly.smallsql.rdb.sql.expression.function.numeric.*;
import io.leavesfly.smallsql.rdb.sql.expression.function.string.*;
import io.leavesfly.smallsql.rdb.sql.expression.function.system.ExpressionFunctionCase;
import io.leavesfly.smallsql.rdb.sql.expression.function.system.ExpressionFunctionIIF;
import io.leavesfly.smallsql.rdb.sql.expression.function.time.*;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;

import java.sql.SQLException;

/**
 * 具体函数工厂实现类
 * 使用工厂模式和策略模式重构函数创建过程
 */
public class ConcreteFunctionFactory implements FunctionFactory {

    @Override
    public Expression createFunction(int functionType) throws SQLException {
        switch (functionType) {
            // numeric functions:
            case SQLTokenizer.ABS:
                return new ExpressionFunctionAbs();
            case SQLTokenizer.ACOS:
                return new ExpressionFunctionACos();
            case SQLTokenizer.ASIN:
                return new ExpressionFunctionASin();
            case SQLTokenizer.ATAN:
                return new ExpressionFunctionATan();
            case SQLTokenizer.ATAN2:
                return new ExpressionFunctionATan2();
            case SQLTokenizer.CEILING:
                return new ExpressionFunctionCeiling();
            case SQLTokenizer.COS:
                return new ExpressionFunctionCos();
            case SQLTokenizer.COT:
                return new ExpressionFunctionCot();
            case SQLTokenizer.DEGREES:
                return new ExpressionFunctionDegrees();
            case SQLTokenizer.EXP:
                return new ExpressionFunctionExp();
            case SQLTokenizer.FLOOR:
                return new ExpressionFunctionFloor();
            case SQLTokenizer.LOG:
                return new ExpressionFunctionLog();
            case SQLTokenizer.LOG10:
                return new ExpressionFunctionLog10();
            case SQLTokenizer.MOD:
                return new ExpressionFunctionMod();
            case SQLTokenizer.PI:
                return new ExpressionFunctionPI();
            case SQLTokenizer.POWER:
                return new ExpressionFunctionPower();
            case SQLTokenizer.RADIANS:
                return new ExpressionFunctionRadians();
            case SQLTokenizer.RAND:
                return new ExpressionFunctionRand();
            case SQLTokenizer.ROUND:
                return new ExpressionFunctionRound();
            case SQLTokenizer.SIN:
                return new ExpressionFunctionSin();
            case SQLTokenizer.SIGN:
                return new ExpressionFunctionSign();
            case SQLTokenizer.SQRT:
                return new ExpressionFunctionSqrt();
            case SQLTokenizer.TAN:
                return new ExpressionFunctionTan();
            case SQLTokenizer.TRUNCATE:
                return new ExpressionFunctionTruncate();

            // string functions:
            case SQLTokenizer.ASCII:
                return new ExpressionFunctionAscii();
            case SQLTokenizer.BITLEN:
                return new ExpressionFunctionBitLen();
            case SQLTokenizer.CHARLEN:
            case SQLTokenizer.CHARACTLEN:
                return new ExpressionFunctionCharLen();
            case SQLTokenizer.CHAR:
                return new ExpressionFunctionChar();
            case SQLTokenizer.DIFFERENCE:
                return new ExpressionFunctionDifference();
            case SQLTokenizer.INSERT:
                return new ExpressionFunctionInsert();
            case SQLTokenizer.LCASE:
                return new ExpressionFunctionLCase();
            case SQLTokenizer.LEFT:
                return new ExpressionFunctionLeft();
            case SQLTokenizer.LENGTH:
                return new ExpressionFunctionLength();
            case SQLTokenizer.LOCATE:
                return new ExpressionFunctionLocate();
            case SQLTokenizer.LTRIM:
                return new ExpressionFunctionLTrim();
            case SQLTokenizer.OCTETLEN:
                return new ExpressionFunctionOctetLen();
            case SQLTokenizer.REPEAT:
                return new ExpressionFunctionRepeat();
            case SQLTokenizer.REPLACE:
                return new ExpressionFunctionReplace();
            case SQLTokenizer.RIGHT:
                return new ExpressionFunctionRight();
            case SQLTokenizer.RTRIM:
                return new ExpressionFunctionRTrim();
            case SQLTokenizer.SPACE:
                return new ExpressionFunctionSpace();
            case SQLTokenizer.SOUNDEX:
                return new ExpressionFunctionSoundex();
            case SQLTokenizer.SUBSTRING:
                return new ExpressionFunctionSubstring();
            case SQLTokenizer.UCASE:
                return new ExpressionFunctionUCase();

            // date time functions
            case SQLTokenizer.DAYOFMONTH:
                return new ExpressionFunctionDayOfMonth();
            case SQLTokenizer.DAYOFWEEK:
                return new ExpressionFunctionDayOfWeek();
            case SQLTokenizer.DAYOFYEAR:
                return new ExpressionFunctionDayOfYear();
            case SQLTokenizer.HOUR:
                return new ExpressionFunctionHour();
            case SQLTokenizer.MINUTE:
                return new ExpressionFunctionMinute();
            case SQLTokenizer.MONTH:
                return new ExpressionFunctionMonth();
            case SQLTokenizer.YEAR:
                return new ExpressionFunctionYear();

            // system functions:
            case SQLTokenizer.IIF:
                return new ExpressionFunctionIIF();
            case SQLTokenizer.CASE:
                return new ExpressionFunctionCase();

            default:
                Object[] params = {SQLTokenizer.getKeyWord(functionType)};
                throw SmallSQLException.create(Language.STXADD_FUNC_UNKNOWN, params);
        }
    }
}