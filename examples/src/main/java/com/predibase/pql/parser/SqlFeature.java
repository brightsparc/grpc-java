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

import com.google.common.collect.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.util.*;

import java.util.*;
import java.util.function.*;

import static java.util.Objects.*;

/**
 * A <code>SqlFeature</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlPredict</code> clause.
 *
 * <p>Basic given grammar is: given_name=given_value.
 * The given key relates to a field in the select clause.
 * The given value can be ome of the following types:
 *
 * <ul>
 *   <li>simple identifier</li>
 *   <li>integer literal</li>
 *   <li>string literal</li>
 *   <li>set of values</li>
 *   <li>range of values</li>
 *   <li>select clause</li>
 * </ul>
 */
public class SqlFeature extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("FEATURE", SqlKind.HINT) {
        @Override public SqlCall createCall(
            SqlLiteral functionQualifier,
            SqlParserPos pos,
            SqlNode... operands) {
          return new SqlFeature(pos,
              (SqlIdentifier) requireNonNull(operands[0], "name"),
              ((SqlLiteral) requireNonNull(operands[1], "featureType"))
                  .getValueAs(FeatureType.class),
              (SqlNodeList) operands[2],
              (SqlNodeList) operands[3]);
        }
      };


  //~ Enums ------------------------------------------------------------------

  /**
   * The feature type.
   */
  public enum FeatureType implements Symbolizable {
     /**
     * Binary features are directly transformed into a binary valued vector of length n.
     */
    BINARY,
    /**
     * Numerical features are directly transformed into a float valued vector of length n.
     */
    NUMERIC,
    /**
     * Category features are transformed into an integer valued vector of size n.
     */
    CATEGORY,
    /**
     * Set features are expected to be provided as a string of elements separated by whitespace.
     */
    SET,
    /**
     * Bag features are expected to be provided as a string of elements separated by whitespace.
     */
    BAG,
    /**
     * Sequence features are transformed into an integer valued matrix of size n x l.
     */
    SEQUENCE,
    /**
     * Text features are treated in the same way of sequence features, with a couple differences.
     */
    TEXT,
    /**
     * Timeseries features are treated in the same way of sequence features.
     */
    TIMESERIES,
    /**
     * Ludwig supports reads in audio files using Python's library SoundFile
     * therefore supporting WAV, FLAC, OGG and MAT files.
     */
    AUDIO,
    /**
     * Ludwig supports both grayscale and color images. The number of channels is inferred,
     * but make sure all your images have the same number of channels. During preprocessing,
     * raw image files are transformed into numpy ndarrays and saved in the hdf5 format.
     */
    IMAGE,
    /**
     * Ludwig will try to infer the date format automatically, but a specific format
     * can be provided.
     */
    DATE,
    /**
     * GEOMETRY is a indexing system for representing geospatial data.
     */
    GEOMETRY,
    /**
     * Vector features allow to provide an ordered set of numerical values all at once.
     */
    VECTOR
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier name;
  private final FeatureType featureType;
  private final SqlNodeList encoder;
  private final SqlNodeList decoder;

  //~ Constructors -----------------------------------------------------------

  public SqlFeature(
      SqlParserPos pos,
      SqlIdentifier name,
      FeatureType featureType,
      SqlNodeList encoder,
      SqlNodeList decoder) {
    super(pos);
    this.name = Objects.requireNonNull(name, "name");
    this.featureType = Objects.requireNonNull(featureType, "featureType");
    this.encoder = encoder;
    this.decoder = decoder;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, featureType.symbol(SqlParserPos.ZERO), encoder, decoder);
  }

  /** Returns the name. */
  public String getName() {
    return name.getSimple();
  }

  /** Returns the feature type. */
  public FeatureType getGivenType() {
    return featureType;
  }

  /** Returns the encoder. */
  public SqlNodeList getEncoder() {
    return encoder;
  }

  /** Returns the decoder. */
  public SqlNodeList getDecoder() {
    return decoder;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    writer.keyword(featureType.toString());
    if (encoder != null || decoder != null) {
      writer.keyword("WITH");
    }
    if (encoder != null) {
      writer.keyword("ENCODER");
      // If is identifier write out name prefix
      SqlWriter.Frame frame = writer.startList("(", ")");
      encoder.forEach(e -> {
        writer.sep(",");
        e.unparse(writer, leftPrec, rightPrec);
      });
      writer.endList(frame);
    }
    if (decoder != null) {
      writer.keyword("DECODER");
      // If is identifier write out name prefix
      SqlWriter.Frame frame = writer.startList("(", ")");
      decoder.forEach(d -> {
        writer.sep(",");
        d.unparse(writer, leftPrec, rightPrec);
      });
      writer.endList(frame);
    }
  }

  /** Calls an action for each (name, type) pair from {@code columnList}, in which
   * they alternate. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void forEachFeatureOption(List list, BiConsumer<SqlIdentifier, SqlLiteral> consumer) {
    Pair.forEach((List<SqlIdentifier>) Util.quotientList(list, 2, 0),
        Util.quotientList((List<SqlLiteral>) list, 2, 1), consumer);
  }
}
