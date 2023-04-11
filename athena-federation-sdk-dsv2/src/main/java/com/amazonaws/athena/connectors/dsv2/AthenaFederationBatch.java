/*-
 * #%L
 * athena-federation-sdk-dsv2
 * %%
 * Copyright (C) 2023 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.dsv2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.ArrowUtils;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class AthenaFederationBatch implements Batch
{
    private final AthenaFederationAdapterDefinition federationAdapterDefinition;
    private final Map<String, String> properties;
    private final String getTableLayoutRequestSerialiedString;

    private static final long blockSize = Long.MAX_VALUE;

    public AthenaFederationBatch(
        AthenaFederationAdapterDefinition federationAdapterDefinition,
        Map<String, String> properties,
        String getTableLayoutRequestSerialiedString)
    {
        this.federationAdapterDefinition = federationAdapterDefinition;
        this.properties = properties;
        this.getTableLayoutRequestSerialiedString = getTableLayoutRequestSerialiedString;
    }

    // Returns a list of input partitions. Each InputPartition represents a data split that can be processed by one Spark task. The number of input partitions returned here is the same as the number of RDD partitions this scan outputs.
    // If the Scan supports filter pushdown, this Batch is likely configured with a filter and is responsible for creating splits for that filter, which is not a full scan.
    // This method will be called only once during a data source scan, to launch one Spark job.
    @Override
    public InputPartition[] planInputPartitions()
    {
        MetadataHandler metadataHandler = federationAdapterDefinition.getMetadataHandler(
            federationAdapterDefinition.getFederationConfig(properties));

        // Automatically close out this blockAllocator since the end result is a serialized ReadRecordsRequest
        // which does not require the allocated blocks to exist anymore.
        try (BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator())) {
            GetTableLayoutRequest layoutReq = (GetTableLayoutRequest) ProtobufSerDe.buildFromJson(getTableLayoutRequestSerialiedString, GetTableLayoutRequest.newBuilder());
            GetTableLayoutResponse layoutResponse = metadataHandler.doGetTableLayout(blockAllocator, layoutReq);

            // Lambda to get splits given a continuation token
            BiFunction<String, Boolean, GetSplitsResponse> getSplits = (continuationToken, start) -> {
                if (!start && continuationToken == null) {
                    return null;
                }

                try {
                    return metadataHandler.doGetSplits(
                        blockAllocator,
                        GetSplitsRequest.newBuilder()
                            .setIdentity(layoutReq.getIdentity())
                            .setQueryId(layoutReq.getQueryId())
                            .setCatalogName(layoutReq.getCatalogName())
                            .setTableName(layoutReq.getTableName())
                            .setPartitions(layoutResponse.getPartitions())
                            .addAllPartitionCols(layoutReq.getPartitionColsList())
                            .setConstraints(layoutReq.getConstraints())
                            .setContinuationToken(continuationToken)
                            .build());
                }
                catch (Exception ex) {
                    // We have to catch and rethrow as unchecked because we are inside of a lambda
                    throw new RuntimeException(ex);
                }
            };

            // Grab all the splits from all the pages
            Stream<AthenaFederationInputPartition> allInputPartitions = Stream.empty();
            for (
                GetSplitsResponse currentResponse = getSplits.apply(null, true);
                currentResponse != null;
                currentResponse = getSplits.apply(currentResponse.getContinuationToken(), false)
            ) {
                  Stream<AthenaFederationInputPartition> currentInputPartitions = currentResponse.getSplitsList().stream()
                      .map(split -> {
                          try {
                               return AthenaFederationInputPartition.fromReadRecordsRequest(
                                    ReadRecordsRequest.newBuilder()
                                        .setIdentity(layoutReq.getIdentity())
                                        .setCatalogName(layoutReq.getCatalogName())
                                        .setQueryId(layoutReq.getQueryId())
                                        .setTableName(layoutReq.getTableName())
                                        .setSchema(layoutReq.getSchema())
                                        .setSplit(split)
                                        .setConstraints(layoutReq.getConstraints())
                                        // Setting both of these to be equal should disable spilling
                                        .setMaxBlockSize(blockSize)
                                        .setMaxInlineBlockSize(blockSize)
                                        .build());
                          }
                          catch (InvalidProtocolBufferException ex) {
                              // Lambda is not allowed to throw a checked exception so we have to rethrow here as an unchecked
                              throw new RuntimeException(ex);
                          }
                      });
                  allInputPartitions = Stream.concat(allInputPartitions, currentInputPartitions);
              }
              return allInputPartitions.toArray(InputPartition[]::new);
        }
        catch (Exception ex) {
            // We must catch and rethrow here because the interface of `planInputPartitions` does not
            // declare any exceptions thrown.
            throw new RuntimeException(ex);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        // This factory is serialized to all the executors and then createColumnarReader is run on each executor
        return new AthenaFederationPartitionReaderFactory(properties, federationAdapterDefinition);
    }
}
