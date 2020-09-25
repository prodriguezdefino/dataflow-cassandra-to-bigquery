/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.example.cass2bq;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/**
 * This Dataflow Template performs a one off copy of one table from Apache Cassandra to BigQuery.
 *
 * The minimum required configuration required to run the pipeline is:
 *
 * <ul>
 * <li><b>cassandraHosts:</b> The hosts of the Cassandra nodes in a comma separated value list.
 * <li><b>cassandraPort:</b> The tcp port where Cassandra can be reached on the nodes.
 * <li><b>cassandraKeyspace:</b> The Cassandra keyspace where the table is located.
 * <li><b>cassandraTable:</b> The Cassandra table to be copied.
 * <li><b>outputTableName:</b> FQN of the destination BigQuery table (project:dataset.table format).
 * </ul>
 */
final class CassandraToBigQueryPipeline {

  private static final Duration WINDOW_DURATION = Duration.standardMinutes(1);

  public interface Options extends PipelineOptions {

    @Description("Cassandra hosts to read from")
    ValueProvider<String> getCassandraHosts();

    @SuppressWarnings("unused")
    void setCassandraHosts(ValueProvider<String> hosts);

    @Description("Cassandra port")
    @Default.Integer(9042)
    ValueProvider<Integer> getCassandraPort();

    @SuppressWarnings("unused")
    void setCassandraPort(ValueProvider<Integer> port);

    @Description("Cassandra keyspace to read from")
    ValueProvider<String> getCassandraKeyspace();

    @SuppressWarnings("unused")
    void setCassandraKeyspace(ValueProvider<String> keyspace);

    @Description("Cassandra table to read from")
    ValueProvider<String> getCassandraTable();

    @SuppressWarnings("unused")
    void setCassandraTable(ValueProvider<String> cassandraTable);

    @Description("Streaming flag")
    @Default.Boolean(true)
    Boolean getIsStreamingBQIngestion();

    void setIsStreamingBQIngestion(Boolean value);

    @Validation.Required
    @Description("BigQuery output table name")
    ValueProvider<String> getOutputTableName();

    void setOutputTableName(ValueProvider<String> value);

    @Description("Number of files that will be use to shard the upload into BigQuery")
    @Default.Integer(0)
    Integer getNumFileShards();

    void setNumFileShards(Integer value);

    @Validation.Required
    @Description("GCS Temp location, in use by BQ file based ingestion")
    ValueProvider<String> getGCSTempLocation();

    void setGCSTempLocation(ValueProvider<String> value);

  }

  /**
   * Runs a pipeline to copy one Cassandra table to BigQuery.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    var pipeline = Pipeline.create(options);

    pipeline
            .apply("Read from Cassandra", getDataSource(options))
            .apply("Ingest to BigQuery", getBQWriter(options));

    pipeline.run();
  }

  private static PTransform<PBegin, PCollection<Row>> getDataSource(Options options) {
    // Split the Cassandra Hosts value provider into a list value provider.
    var hosts
            = ValueProvider.NestedValueProvider.of(
                    options.getCassandraHosts(),
                    (SerializableFunction<String, List<String>>) value -> Arrays.asList(value.split(",")));

    // Create a factory method to inject the CassandraRowMapperFn to allow custom type mapping.
    var cassandraObjectMapperFactory
            = new CassandraRowMapperFactory(options.getCassandraTable(), options.getCassandraKeyspace());

    return CassandraIO.<Row>read()
            .withHosts(hosts)
            .withPort(options.getCassandraPort())
            .withKeyspace(options.getCassandraKeyspace())
            .withTable(options.getCassandraTable())
            .withMapperFactoryFn(cassandraObjectMapperFactory)
            .withEntity(Row.class)
            .withCoder(SerializableCoder.of(Row.class));
  }

  private static BigQueryIO.Write<Row> getBQWriter(Options options) {

    var bqWriter = BigQueryIO.<Row>write()
            .to(options.getOutputTableName())
            .withoutValidation()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withFormatFunction(BigQueryUtils.toTableRow());
    /**
     * Based on user input, we decide to do either batch or stream data into BigQuery. In case of FILE_UPLOADS(batching), we save them as
     * temporary files in GCS and then load into BigQuery table at a specified frequency.
     */
    if (options.getIsStreamingBQIngestion()) {
      bqWriter = bqWriter.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);
    } else {
      bqWriter = bqWriter
              .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
              .withCustomGcsTempLocation(options.getGCSTempLocation())
              .withTriggeringFrequency(WINDOW_DURATION)
              .withNumFileShards(options.getNumFileShards());
    }
    return bqWriter;
  }
}
