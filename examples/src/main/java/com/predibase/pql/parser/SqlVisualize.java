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

/**
 * A <code>SqlMetrics</code> is a node of a parse tree which represents
 * the instruction to show metrics.
 */
public class SqlVisualize extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("VISUALIZE", SqlKind.OTHER);

  //~ Enums ------------------------------------------------------------------

  /**
   * The prediction type (either predict or evaluate).
   */
  public enum VisualizeType implements Symbolizable {
    /**
     * No visualization type defined.
     */
    UNDEFINED,
    /**
     * Produces a line chart plotting the roc curves for the specified target.
     */
    ROC_CURVES,
    /**
     * Produces a heatmap of the confusion matrix in the predictions for each field that
     * has a confusion matrix. The value of top_n_classes limits the heatmap to the n most
     * frequent classes.
     */
    CONFUSION_MATRIX,
    /**
     * Produces a residuals plot of the predictions compare with the ground truth for each model.
     */
    RESIDUALS,
    /**
     * Produces a line plot showing how that measure changed over the course of the epochs
     * of training on the training and validation sets.  If target feature is not specified,
     * then all output features are plotted.
     */
    LEARNING_CURVES,
    /**
     * Produces bars in a bar plot, one for each overall metric available in the test_statistics
     * for the specified target feature.
     */
    COMPARE_PERFORMANCE,
    /**
     * This visualization produces a pie chart comparing the predictions of the two models
     * for the specified target.
     */
    COMPARE_CLASSIFIERS_PREDICTIONS,
    /**
     * This visualization produces a radar plot comparing the distributions of predictions
     * of the models for the first 10 classes of the specified target.
     */
    COMPARE_CLASSIFIERS_PREDICTIONS_DISTRIBUTION,
    /**
     * Produces a pair of lines indicating the accuracy of the model and the data coverage
     * while increasing a threshold (x axis) on the probabilities of predictions for the specified
     * target.
     */
    CONFIDENCE_THRESHOLDING,
    /**
     * Target needs to be exactly two, either category or binary. Three plots are produced.
     * The first plot shows several semi transparent lines. They summarize the 3d surfaces
     * displayed by confidence_thresholding_2thresholds_3d that have thresholds on the confidence
     * of the predictions of the two threshold_output_feature_names as x and y axes and either
     * the data coverage percentage or the accuracy as z axis. Each line represents a slice of
     * the data coverage surface projected onto the accuracy surface.
     */
    CONFIDENCE_THRESHOLDING_2THRESHOLDS_2D,
    /**
     * Target needs to be exactly two, either category or binary.
     * The plot shows the 3d surfaces displayed by confidence_thresholding_2thresholds_3d
     * that have thresholds on the confidence of the predictions of the two
     * targets as x and y axes and either the data coverage percentage or the accuracy as z axis.
     */
    CONFIDENCE_THRESHOLDING_2THRESHOLDS_3D,
    /**
     * Produces a line indicating the accuracy of the model and the data coverage while
     * increasing a threshold on the probabilities of predictions for the specified target.
     */
    CONFIDENCE_THRESHOLDING_DATA_VS_ACC,
    /**
     * Produces a line chart plotting a threshold on the confidence of the model against the
     * metric for the specified target.
     */
    BINARY_THRESHOLD_VS_METRIC,
    /**
     * For each class or each of the n most frequent classes if top_n_classes is specified,
     * it produces two plots computed on the fly from the probabilities of predictions for
     * the specified target.
     */
    CALIBRATION_1_VS_ALL,
    /**
     * For each class, produces two plots computed on the fly from the probabilities of
     * predictions for the specified target.
     */
    CALIBRATION_MULTICLASS,
    /**
     * Generates plots for top_n_classes. The first plot is a line plot with one x axis
     * representing the different classes and two vertical axes colored in orange and blue
     * respectively. The orange one is the frequency of the class and an orange line is
     * plotted to show the trend. The blue one is the F1 score for that class and a blue line
     * is plotted to show the trend. The classes on the x axis are sorted by f1 score.
     */
    FREQUENCY_VS_F1,
    /**
     * The visualization creates one plot for each hyper-parameter in the file at
     * plus an additional one containing a pair plot of hyper-parameters interactions.
     */
    HYPEROPT_REPORT,
  }

  //~ Instance fields --------------------------------------------------------

  public final SqlNodeList format;
  public final SqlNodeList metricList;
  public final SqlNodeList targetList;
  public final SqlNodeList modelList;
  public final VisualizeType visualizeType;
  public final SqlNode given;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlVisualize. */
  public SqlVisualize(SqlParserPos pos, VisualizeType visualizeType, SqlNodeList format,
      SqlNodeList metricList, SqlNodeList targetList, SqlNodeList modelList, SqlNode given) {
    super(pos);
    this.visualizeType = visualizeType;
    this.format = format;
    this.metricList = metricList;
    this.targetList = targetList;
    this.modelList = Objects.requireNonNull(modelList, "modelList");
    this.given = given;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Pure
  public final VisualizeType getVisualizeType() {
    return visualizeType;
  }

  @Pure
  public final SqlNodeList getFormat() {
    return format;
  }

  @Pure
  public final SqlNodeList getMetricList() {
    return metricList;
  }

  @Pure
  public final SqlNodeList getTargetList() {
    return targetList;
  }

  @Pure
  public final SqlNodeList getModelList() {
    return modelList;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(visualizeType.symbol(SqlParserPos.ZERO), format,
        metricList, targetList, modelList, given);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("VISUALIZE");
    writer.keyword(visualizeType.name());
    if (format != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      format.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
    if (metricList != null) {
      writer.keyword("METRICS");
      if (metricList.size() == 1) {
        metricList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        metricList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
    }
    if (targetList != null) {
      writer.keyword("TARGET");
      if (targetList.size() == 1) {
        targetList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        targetList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
    }
    if (modelList.size() > 0) {
      writer.keyword("USING"); // Is this FROM model or USING ?
      if (modelList.size() == 1) {
        modelList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        modelList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
    }
    if (given != null) {
      writer.keyword("GIVEN");
      given.unparse(writer, leftPrec, rightPrec);
    }
  }

}
