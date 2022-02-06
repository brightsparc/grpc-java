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
import org.checkerframework.checker.nullness.qual.*;

import java.util.*;

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
      new SqlSpecialOperator("FEATURE", SqlKind.OTHER) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlFeature(pos,
              (SqlIdentifier) requireNonNull(operands[0], "name"),
              ((SqlLiteral) requireNonNull(operands[1], "featureType"))
                  .getValueAs(FeatureType.class),
              (SqlNodeList) operands[2],
              (SqlNodeList) operands[3],
              (SqlNodeList) operands[4]);
        }
      };


  //~ Enums ------------------------------------------------------------------

  /**
   * The feature type.
   */
  public enum FeatureType implements Symbolizable {
    /**
     * Feature type is undefined.
     */
    UNDEFINED,
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
     * H3 is a indexing system for representing geospatial data.
     * For more details about it refer to: https://eng.uber.com/h3/.
     */
    H3,
    /**
     * Vector features allow to provide an ordered set of numerical values all at once.
     */
    VECTOR
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier name;
  private final FeatureType featureType;
  private final SqlNodeList preprocessing;
  private final SqlNodeList encoder;
  private final SqlNodeList decoder;

  //~ Constructors -----------------------------------------------------------

  public static SqlIdentifier createIdentifier(SqlIdentifier parent, FeatureType feature,
      SqlParserPos pos) {
    if (parent == null) {
      return new SqlIdentifier(feature.name(), pos);
    }
    return new SqlIdentifier(
        ImmutableList.<String>builder().addAll(parent.names).add(feature.name()).build(), pos);
  }

  public SqlFeature(SqlParserPos pos, SqlIdentifier name, FeatureType featureType,
      SqlNodeList preprocessing,  SqlNodeList encoder, SqlNodeList decoder) {
    super(pos);
    this.name = Objects.requireNonNull(name, "name");
    this.featureType = Objects.requireNonNull(featureType, "featureType");
    this.preprocessing = preprocessing;
    this.encoder = encoder;
    this.decoder = decoder;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, featureType.symbol(SqlParserPos.ZERO),
        preprocessing, encoder, decoder);
  }

  /** Returns the name. */
  public SqlIdentifier getName() {
    return name;
  }

  /** Returns the given name as a type. */
  public <T extends Object> T getNameAs(Class<T> clazz) {
    if (clazz.isInstance(name)) {
      return clazz.cast(name);
    }
    // If we are asking for a string, get the simple name, or use
    if (clazz == String.class) {
      if (name.isSimple()) {
        return clazz.cast(name.getSimple());
      }
      return clazz.cast(name.toString());
    }
    throw new AssertionError("cannot cast " + name + " as " + clazz);
  }

  /** Returns the feature type. */
  public FeatureType getGivenType() {
    return featureType;
  }

  /** Returns the encoder. */
  public List<SqlGivenItem> getPreprocessing() {
    return getGivenItems(preprocessing);
  }

  /** Returns the encoder. */
  public List<SqlGivenItem> getEncoder() {
    return getGivenItems(encoder);
  }

  /** Returns the decoder. */
  public List<SqlGivenItem> getDecoder() {
    return getGivenItems(decoder);
  }

  /** Adds items and nested items. */
  private List<SqlGivenItem> getGivenItems(SqlNodeList items) {
    ArrayList<SqlGivenItem> list = new ArrayList<>();
    if (items != null) {
      items.forEach(i -> {
        if (i instanceof  SqlGivenItem) {
          list.add((SqlGivenItem) i);
        } else if (i instanceof SqlBasicCall) {
          // Named argument
          SqlNode left = ((SqlBasicCall) i).getOperandList().get(0);
          if (left instanceof  SqlNodeList) {
            list.addAll(getGivenItems((SqlNodeList) left));
          }
        }
      });
    }
    return list;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    writer.keyword(featureType.toString());
    if (preprocessing != null || encoder != null || decoder != null) {
      writer.keyword("WITH");
    }
    // Should we write processor first
    if (preprocessing != null) {
      writer.keyword("PREPROCESSING");
      SqlWriter.Frame frame = writer.startList("(", ")");
      preprocessing.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
    if (encoder != null) {
      writer.keyword("ENCODER");
      // If is identifier write out name prefix
      SqlWriter.Frame frame = writer.startList("(", ")");
      encoder.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
    if (decoder != null) {
      writer.keyword("DECODER");
      // If is identifier write out name prefix
      SqlWriter.Frame frame = writer.startList("(", ")");
      decoder.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
  }
}
