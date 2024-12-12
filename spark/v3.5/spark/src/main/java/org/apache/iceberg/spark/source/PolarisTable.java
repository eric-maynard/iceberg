/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class PolarisTable
    implements org.apache.spark.sql.connector.catalog.Table, SupportsRead, SupportsWrite {

  public static final String POLARIS_SOURCE_PROPERTY = "_source";

  private final String name;
  private final Map<String, String> properties;
  private final Dataset<Row> df;

  public PolarisTable(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;

    org.apache.spark.sql.SparkSession spark =
        org.apache.spark.sql.SparkSession.getActiveSession().get();
    org.apache.spark.sql.DataFrameReader reader = spark.read();

    this.df =
        reader.format(this.properties.get(POLARIS_SOURCE_PROPERTY)).options(this.properties).load();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    return new PolarisScanBuilder(this.df);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    return new PolarisWriteBuilder(this.properties, logicalWriteInfo);
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public StructType schema() {
    return df.schema();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
  }

  public static class PolarisScanBuilder implements ScanBuilder {
    private final org.apache.spark.sql.Dataset<?> df;

    public PolarisScanBuilder(org.apache.spark.sql.Dataset<?> df) {
      this.df = df;
    }

    @Override
    public Scan build() {
      return new PolarisScan(this.df);
    }
  }

  public static class PolarisScan implements Scan {
    private final org.apache.spark.sql.Dataset<?> df;

    public PolarisScan(org.apache.spark.sql.Dataset<?> df) {
      this.df = df;
    }

    @Override
    public StructType readSchema() {
      return df.schema();
    }
  }

  public static class PolarisWriteBuilder implements WriteBuilder {
    private final Map<String, String> properties;
    private final LogicalWriteInfo logicalWriteInfo;
    private final SparkSession spark;

    public PolarisWriteBuilder(Map<String, String> properties, LogicalWriteInfo logicalWriteInfo) {
      this.properties = properties;
      this.logicalWriteInfo = logicalWriteInfo;
      this.spark = SparkSession.builder().getOrCreate();
    }

    @Override
    public Write build() {
      return getWriteImplementation(this.properties.get("format"), this.properties);
    }

    public Write getWriteImplementation(String format, Map<String, String> options) {
      try {
        String className = normalizeFormat(format) + "Write";
        Class<?> writeClass =
            tryLoadClass("org.apache.spark.sql.execution.datasources.v2." + className);
        if (writeClass == null) {
          writeClass = tryLoadClass("org.apache.spark.sql.sources." + className);
        }
        if (writeClass == null) {
          throw new UnsupportedOperationException(
              "No Write implementation found for format: " + format);
        }
        if (!Write.class.isAssignableFrom(writeClass)) {
          throw new UnsupportedOperationException(
              className + " does not implement Write interface.");
        }
        Constructor<?> constructor = writeClass.getConstructor(SparkSession.class, Map.class);
        return (Write) constructor.newInstance(spark, options);

      } catch (Exception e) {
        throw new UnsupportedOperationException(
            "Error loading Write implementation for format: " + format, e);
      }
    }

    private String normalizeFormat(String format) {
      return format.substring(0, 1).toUpperCase() + format.substring(1);
    }

    private Class<?> tryLoadClass(String className) {
      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        return null;
      }
    }
  }
}
