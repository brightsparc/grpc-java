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
 * Parse tree for {@code CREATE MODEL} statement.
 */
public class SqlCreateModel extends SqlCreate {
  public final SqlIdentifier name;
  public final SqlNode config;
  public final SqlNodeList featureList;
  public final SqlNodeList targetList;
  public final SqlNodeList preprocessing;
  public final SqlNodeList trainer;
  public final SqlNodeList combiner;
  public final SqlDatasetRef sourceRef;
  public final SqlNode query;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE MODEL", SqlKind.OTHER_DDL);

  /** Creates a SqlCreateModel with config string. */
  public SqlCreateModel(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNode config, SqlDatasetRef sourceRef, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.config = config;
    // TODO: Based on config, dynamically set the following properties
    this.featureList = null;
    this.targetList = null;
    this.preprocessing = null;
    this.combiner = null;
    this.trainer = null;
    this.sourceRef = sourceRef;
    this.query = query;
  }

  /** Creates a SqlCreateModel with sql parameters. */
  public SqlCreateModel(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNodeList featureList, SqlNodeList targetList,
      SqlNodeList preprocessing, SqlNodeList combiner, SqlNodeList trainer,
      SqlDatasetRef sourceRef, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.config = null;
    this.featureList = Objects.requireNonNull(featureList, "featureList");
    this.targetList = Objects.requireNonNull(targetList, "targetList");
    this.preprocessing = preprocessing;
    this.combiner = combiner;
    this.trainer = trainer;
    this.sourceRef = sourceRef;
    this.query = query;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, config,
        targetList, featureList, preprocessing, combiner, trainer, sourceRef, query);
  }

  @Pure
  public final SqlIdentifier getName() {
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

  @Pure
  public final SqlNode getConfig() {
    return config;
  }

  @Pure
  public final List<SqlIdentifier> getTargetList() {
    if (targetList != null) {
      return targetList.stream().map(t -> (SqlIdentifier) t).collect(Collectors.toList());
    }
    return new ArrayList<>();
  }

  @Pure
  public final List<SqlFeature> getFeatureList() {
    if (featureList != null) {
      return featureList.stream().map(f -> (SqlFeature) f).collect(Collectors.toList());
    }
    return new ArrayList<>();
  }

  @Pure
  public final List<SqlGivenItem> getPreprocessing() {
    return getGivenItems(preprocessing);
  }

  @Pure
  public final List<SqlGivenItem> getCombiner() {
    return getGivenItems(combiner);
  }

  @Pure
  public final List<SqlGivenItem> getTrainer() {
    return getGivenItems(trainer);
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

  @Pure
  public final SqlDatasetRef getSourceRef() {
    return sourceRef;
  }

  @Pure
  public final SqlNode getQuery() {
    return query;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("MODEL");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    // Should config be one of the other?
    if (config != null) {
      writer.newlineAndIndent();
      writer.keyword("WITH");
      writer.keyword("CONFIG");
      config.unparse(writer, leftPrec, rightPrec);
    } else {
      // Write the list of features
      SqlWriter.Frame featureFrame = writer.startList("(", ")");
      featureList.forEach(feature -> {
        writer.sep(",");
        writer.newlineAndIndent();
        feature.unparse(writer, leftPrec, rightPrec);
      });
      writer.endList(featureFrame);
      writer.newlineAndIndent();
      writer.keyword("TARGET");
      if (targetList.size() == 1) {
        targetList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        targetList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
      if (preprocessing != null || combiner != null || trainer != null) {
        writer.keyword("WITH");
        if (preprocessing != null) {
          writer.newlineAndIndent();
          writer.keyword("PREPROCESSING");
          SqlWriter.Frame frame = writer.startList("(", ")");
          preprocessing.unparse(writer, leftPrec, rightPrec);
          writer.endList(frame);
        }
        if (combiner != null) {
          writer.newlineAndIndent();
          writer.keyword("COMBINER");
          SqlWriter.Frame frame = writer.startList("(", ")");
          combiner.unparse(writer, leftPrec, rightPrec);
          writer.endList(frame);
        }
        if (trainer != null) {
          writer.newlineAndIndent();
          writer.keyword("TRAINER");
          SqlWriter.Frame frame = writer.startList("(", ")");
          trainer.unparse(writer, leftPrec, rightPrec);
          writer.endList(frame);
        }
      }
    }
    // From source table, or query
    if (sourceRef != null) {
      writer.newlineAndIndent();
      writer.keyword("FROM");
      sourceRef.unparse(writer, leftPrec, rightPrec);
    } else if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, leftPrec, rightPrec);
    }
  }
}
