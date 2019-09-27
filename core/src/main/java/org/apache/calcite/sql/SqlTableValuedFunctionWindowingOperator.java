/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.*;

import java.util.ArrayList;
import java.util.List;


/**
 * Base class for table-valued function windowing operator (TUMBLE, HOP and SESSION).
 */
public class SqlTableValuedFunctionWindowingOperator extends SqlFunction {
  public SqlTableValuedFunctionWindowingOperator(String name,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
  }

  /**
   * The first parameter of table-value function windowing is a TABLE parameter,
   * which is not scalar. So need to override SqlOperator.argumentMustBeScalar.
   */
  @Override public boolean argumentMustBeScalar(int ordinal) {
    if (ordinal == 0) {
      return false;
    }
    return true;
  }

  /**
   * TUMBLE table-valued function's operands type checker: TUMBLE(ROW, COLUMN_LIST, INTERVAL).
   */
  public static final SqlSingleOperandTypeChecker TABLE_VALUED_FUNCTION_TUMBLE_CHECKER =
      new SqlSingleOperandTypeChecker() {
        public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode node,
            int iFormalOperand,
            boolean throwOnFailure) {
          RelDataType type =
              callBinding.getValidator().deriveType(
                  callBinding.getScope(),
                  node);
          if (iFormalOperand == 0) {
            return type.getSqlTypeName() == SqlTypeName.ROW;
          } else if (iFormalOperand == 1) {
            assert node instanceof SqlBasicCall;
            SqlCallBinding callBinding1 =
                new SqlCallBinding(
                    callBinding.getValidator(),
                    callBinding.getScope(),
                    (SqlCall) node);
            ((SqlBasicCall) node).getOperator().checkOperandTypes(callBinding1, true);
            return type.getSqlTypeName() == SqlTypeName.COLUMN_LIST;
          } else {
            return type.getFamily() == SqlTypeFamily.DATETIME_INTERVAL;
          }
        }

        public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          // There should only be three operands, and number of operands are checked before
          // this call.
          for (int i = 0; i < 3; i++) {
            if (!checkSingleOperandType(callBinding, callBinding.operand(i), i, throwOnFailure)) {
              return false;
            }
          }
          return true;
        }

        // TUMBLE(ROW, COLUMN_LIST, INTERVAL) thus operand count is 3.
        public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.of(3);
        }

        public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName;
        }

        public boolean isOptional(int i) {
          return false;
        }

        public Consistency getConsistency() {
          return Consistency.NONE;
        }
      };


  /**
   * Type-inference strategy whereby the result type of a table function call is a ROW,
   * which is combined from the operand #0(TABLE parameter)'s schema and two
   * additional fields:
   *
   * <ol>
   *  <li>window_start. TIMESTAMP type to indicate a window's start.</li>
   *  <li>window_end. TIMESTAMP type to indicate a window's end.</li>
   * </ol>
   */
  public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_WINDOWING =
      opBinding -> {
        RelDataType inputRowType = opBinding.getOperandType(0);
        List<RelDataTypeField> newFields = new ArrayList<>(inputRowType.getFieldList());
        RelDataType timestampType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

        RelDataTypeField windowStartField =
            new RelDataTypeFieldImpl("window_start", newFields.size(), timestampType);
        newFields.add(windowStartField);
        RelDataTypeField windowEndField =
            new RelDataTypeFieldImpl("window_end", newFields.size(), timestampType);
        newFields.add(windowEndField);

        return new RelRecordType(inputRowType.getStructKind(), newFields);
      };
}
