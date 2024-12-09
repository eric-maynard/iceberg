/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark.source;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class PolarisTable
        implements org.apache.spark.sql.connector.catalog.Table,
        SupportsRead {

    public static final String POLARIS_PROPERTY_PREFIX = "polaris.virtual";

    private final String name;
    private final Map<String, String> properties;
    private final Dataset<Row> df;

    public PolarisTable(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(POLARIS_PROPERTY_PREFIX))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(POLARIS_PROPERTY_PREFIX.length()),
                        Map.Entry::getValue
                ));

        org.apache.spark.sql.SparkSession spark =
                org.apache.spark.sql.SparkSession.getActiveSession().get();
        org.apache.spark.sql.DataFrameReader reader =
                spark.read();

        this.properties.forEach(reader::option);
        this.df = reader.format(this.properties.get("format")).load();
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return new PolarisScanBuilder(this.df);
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
        return Set.of(TableCapability.BATCH_READ);
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
}
