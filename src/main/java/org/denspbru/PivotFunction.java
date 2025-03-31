package org.denspbru;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.type.RelDataType;

public class PivotFunction extends SqlFunction {

    public PivotFunction() {
        super("PIVOT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return super.createCall(functionQualifier, pos, operands);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return validator.getTypeFactory().createSqlType(SqlTypeName.ANY);
    }
}
