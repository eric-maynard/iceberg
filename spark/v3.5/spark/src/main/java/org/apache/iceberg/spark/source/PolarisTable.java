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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class PolarisTable
    implements org.apache.spark.sql.connector.catalog.Table, SupportsRead, SupportsWrite {

  public static final String POLARIS_SOURCE_PROPERTY = "_source";

  private final String name;
  private final Map<String, String> properties;
  private final Dataset<Row> df;
  private final SparkTable sparkTable;

  public PolarisTable(String name, Map<String, String> properties, SparkTable sparkTable) {
    this.name = name;
    this.properties = properties;
    this.sparkTable = sparkTable;

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
    return sparkTable.newWriteBuilder(logicalWriteInfo);
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
    private final org.apache.spark.sql.Dataset<Row> df;

    public PolarisScanBuilder(org.apache.spark.sql.Dataset<Row> df) {
      this.df = df;
    }

    @Override
    public Scan build() {
      return new Scan() {

        @Override
        public StructType readSchema() {
          return df.schema();
        }

        @Override
        public Batch toBatch() {
          return new Batch() {
            @Override
            public InputPartition[] planInputPartitions() {
              return new InputPartition[0];
            }

            @Override
            public PartitionReaderFactory createReaderFactory() {
              return new PartitionReaderFactory() {
                @Override
                public PartitionReader<InternalRow> createReader(int partitionId) {
                  return new PartitionReader<InternalRow>() {
                    private final Iterator<Row> iterator = df.collectAsList().iterator();

                    @Override
                    public boolean next() {
                      return iterator.hasNext();
                    }

                    @Override
                    public InternalRow get() {
                      Row row = iterator.next();
                      return InternalRow.fromSeq(row.toSeq());
                    }

                    @Override
                    public void close() {}
                  };
                }
              };
            }
          };
        }
      };
    }
  }
}
