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
package com.predibase.pql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.util.*;
import org.checkerframework.dataflow.qual.*;

import java.util.*;
import java.util.stream.*;

/**
 * A <code>SqlPredict</code> is a node of a parse tree which represents
 * a sql prediction.
 *
 * <p>Basic predict grammar consist of specifying a type.
 * <ul>
 *   <li>predict</li>
 *   <li>evaluate</li>
 * </ul>
 * And providing a target list of one or more columns.
 * You can request predictions come qualified with:
 * <ul>
 *   <li>explanation</li>
 *   <li>confidence</li>
 * </ul>
 * You can optionally insert into a target table.
 * You must provide the model along with optional version
 * And provide one or more given clauses which are either a select
 * or a set <code>SqlGivenItem</code> for a specific column.
 * </p>
 *
 * <code>
 * PREDICT|EVALUATE TARGET, [TARGET] [WITH EXPLANATION|CONFIDENCE]
 * [INTO TABLE]
 * USING MODEL [VERSION NUMBER]
 * GIVEN [SELECT|CONSTANT|SET|RANGE], ...
 * </code>
 */
public class SqlPredict extends SqlCall {
  //~ Enums ------------------------------------------------------------------

  /**
   * The prediction type (either predict or evaluate).
   */
  public enum PredictType implements Symbolizable {
    /**
     * The prediction type is Predict.
     */
    PREDICT,
    /**
     * The prediction type is Evaluate.
     */
    EVALUATE
  }

  /**
   * The prediction with qualifier.
   */
  public enum WithQualifier implements Symbolizable {
    /**
     * Empty qualifier.
     */
    EMPTY,
    /**
     * With explanation.
     */
    EXPLANATION,
    /**
     * With confidence.
     */
    CONFIDENCE,
    /**
     * With visualization.
     */
    VISUALIZATION,
  }

  //~ Instance fields --------------------------------------------------------

  public final PredictType predictType;
  public final SqlNodeList targetList;
  public final WithQualifier withQualifier;
  public final SqlVisualize.VisualizeType visualizeType;
  public final SqlNodeList visualizeFormat;
  public final SqlNode into;
  public final SqlModelRef model;
  public final SqlNodeList given;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlPredict. */
  public SqlPredict(SqlParserPos pos,
                    PredictType predictType,
                    SqlNodeList targetList,
                    WithQualifier withQualifier,
                    SqlVisualize.VisualizeType visualizeType,
                    SqlNodeList visualizeFormat,
                    SqlNode into,
                    SqlModelRef model,
                    SqlNodeList given) {
    super(pos);
    this.predictType =  Objects.requireNonNull(predictType, "predictType");
    this.targetList = Objects.requireNonNull(targetList, "targetList");
    this.withQualifier = Objects.requireNonNull(withQualifier, "withQualifier");
    this.visualizeType = visualizeType;
    this.visualizeFormat = visualizeFormat;
    this.into = into;
    this.model = model;
    this.given = Objects.requireNonNull(given, "given");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return new SqlSpecialOperator(predictType.name(), SqlKind.OTHER);
  }

  @Override public SqlKind getKind() {
    return SqlKind.OTHER;
  }

  @Pure
  public final PredictType getPredictType() {
    return predictType;
  }

  @Pure
  public final WithQualifier getWithQualifier() {
    return withQualifier;
  }

  @Pure
  public final SqlVisualize.VisualizeType getVisualizationType() {
    return visualizeType;
  }

  @Pure
  public final SqlNodeList getVisualizeFormat() {
    return visualizeFormat;
  }

  @Pure
  public final List<SqlIdentifier> getTargetList() {
    if (targetList != null) {
      return targetList.stream().map(t -> (SqlIdentifier) t).collect(Collectors.toList());
    }
    return new ArrayList<>();
  }

  @Pure
  public final SqlNode getInto() {
    return into;
  }

  @Pure
  public final SqlModelRef getModel() {
    return model;
  }

  @Pure
  public final SqlNodeList getGiven() {
    return given;
  }

  /**
   * Return a list of given items.
   * @return List of {@link SqlGivenItem}
   */
  public final Stream<SqlGivenItem> getGivenItems() {
    return given.stream()
        .filter(g -> g instanceof SqlGivenItem)
        .map(g -> (SqlGivenItem) g);
  }

  /**
   * Return a list of given select clauses.
   * @return List of {@link SqlSelect}
   */
  public final Stream<SqlSelect> getGivenSelect() {
    return given.stream()
        .filter(g -> g instanceof  SqlSelect)
        .map(g -> (SqlSelect) g);
  }

  @Override public List<SqlNode> getOperandList() {
    // Return operand list with version as numeric literal
    return ImmutableNullableList.of(predictType.symbol(SqlParserPos.ZERO), targetList,
        withQualifier.symbol(SqlParserPos.ZERO), visualizeType.symbol(SqlParserPos.ZERO),
        visualizeFormat, into, model, given);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!writer.inQuery()) {
      // If this SELECT is the topmost item in a sub-query, introduce a new
      // frame. (The topmost item in the sub-query might be a UNION or
      // ORDER. In this case, we don't need a wrapper frame.)
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
      this.unparseCall(writer, 0, 0);
      writer.endList(frame);
    } else {
      this.unparseCall(writer, leftPrec, rightPrec);
    }
  }

  private void unparseCall(SqlWriter writer, int leftPrec, int rightPrec) {
    // Output either PREDICT or EVALUATE
    writer.keyword(predictType.toString());

    if (targetList.size() == 1) {
      targetList.unparse(writer, leftPrec, rightPrec);
    } else {
      SqlWriter.Frame targetFrame = writer.startList("(", ")");
      targetList.unparse(writer, leftPrec, rightPrec);
      writer.endList(targetFrame);
    }

    if (withQualifier != WithQualifier.EMPTY) {
      writer.keyword("WITH");
      writer.keyword(withQualifier.toString());
      if (visualizeFormat != null) {
        visualizeFormat.unparse(writer, leftPrec, rightPrec);
      }
    }

    if (into != null) {
      writer.keyword("INTO");
      into.unparse(writer, leftPrec, rightPrec);
    }

    if (model != null) {
      writer.newlineAndIndent();
      writer.keyword("USING");
      model.unparse(writer, leftPrec, rightPrec);
    }

    writer.newlineAndIndent();
    writer.keyword("GIVEN");
    given.unparse(writer, leftPrec, rightPrec);

  }
}
