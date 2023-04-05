/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.examples;

import com.amazonaws.athena.connector.lambda.CollectionsUtils;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperUtil;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.lambda.invoke.LambdaFunctionException;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.examples.ExampleMetadataHandler.MAX_SPLITS_PER_REQUEST;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ExampleMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandlerTest.class);

    private BlockAllocatorImpl allocator;
    private ExampleMetadataHandler metadataHandler;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");
        allocator = new BlockAllocatorImpl();
        metadataHandler = new ExampleMetadataHandler(new LocalKeyFactory(),
                mock(AWSSecretsManager.class),
                mock(AmazonAthena.class),
                "spill-bucket",
                "spill-prefix",
                com.google.common.collect.ImmutableMap.of());
        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doListSchemas()
    {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = ListSchemasRequest.newBuilder()
            .setCatalogName("default")
            .setQueryId("queryId")
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();
        
        // ObjectMapperUtil.assertSerialization(req);
        ListSchemasResponse res = metadataHandler.doListSchemaNames(allocator, req);
        // ObjectMapperUtil.assertSerialization(res);
        logger.info("doListSchemas - {}", res.getSchemasList());
        assertFalse(res.getSchemasList().isEmpty());
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        // Test request with unlimited page size
        logger.info("doListTables - Test unlimited page size");
        ListTablesRequest req = ListTablesRequest.newBuilder()
            .setCatalogName("default")
            .setSchemaName("schema")
            .setQueryId("queryId")
            .setPageSize(UNLIMITED_PAGE_SIZE_VALUE)
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();
        
        ListTablesResponse expectedResponse = ListTablesResponse.newBuilder()
            .setCatalogName("default")
            .addAllTables(
                new ImmutableList.Builder<TableName>()
                    .add(new TableName("schema", "table1"))
                    .add(new TableName("schema", "table2"))
                    .add(new TableName("schema", "table3"))
                    .add(new TableName("schema", "table4"))
                    .add(new TableName("schema", "table5"))
                    .build()
                .stream()
                .map(ProtobufMessageConverter::toTableName)
                .collect(Collectors.toList())
            )
            .build();
        
        // ObjectMapperUtil.assertSerialization(req);
        ListTablesResponse res = metadataHandler.doListTables(allocator, req);
        // ObjectMapperUtil.assertSerialization(res);
        logger.info("doListTables - {}", res);
        assertEqualsListTablesResponse(expectedResponse, res);

        // Test first paginated request with pageSize: 3, nextToken: null
        logger.info("doListTables - Test first pagination request");
        req = ListTablesRequest.newBuilder()
            .setCatalogName("default")
            .setSchemaName("schema")
            .setQueryId("queryId")
            .setPageSize(3)
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();

        expectedResponse = ListTablesResponse.newBuilder()
            .setCatalogName("default")
            .addAllTables(
                new ImmutableList.Builder<TableName>()
                    .add(new TableName("schema", "table1"))
                    .add(new TableName("schema", "table2"))
                    .add(new TableName("schema", "table3"))
                    .build()
                .stream()
                .map(ProtobufMessageConverter::toTableName)
                .collect(Collectors.toList())
            )
            .setNextToken("table4")
            .build();

        // ObjectMapperUtil.assertSerialization(req);
        res = metadataHandler.doListTables(allocator, req);
        // ObjectMapperUtil.assertSerialization(res);
        logger.info("doListTables - {}", res);
        assertEqualsListTablesResponse(expectedResponse, res);

        // Test second paginated request with pageSize: 3, nextToken: res.getNextToken()
        logger.info("doListTables - Test second pagination request");
        req = ListTablesRequest.newBuilder()
            .setCatalogName("default")
            .setSchemaName("schema")
            .setQueryId("queryId")
            .setPageSize(3)
            .setNextToken(res.getNextToken())
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();

        expectedResponse = ListTablesResponse.newBuilder()
        .setCatalogName("default")
        .addAllTables(
            new ImmutableList.Builder<TableName>()
                .add(new TableName("schema", "table4"))
                .add(new TableName("schema", "table5"))
                .build()
            .stream()
            .map(ProtobufMessageConverter::toTableName)
            .collect(Collectors.toList())
        )
        .build();

        // ObjectMapperUtil.assertSerialization(req);
        res = metadataHandler.doListTables(allocator, req);
        // ObjectMapperUtil.assertSerialization(res);
        logger.info("doListTables - {}", res);
        assertEqualsListTablesResponse(expectedResponse, res);

        logger.info("doListTables - exit");
    }

    private void assertEqualsListTablesResponse(ListTablesResponse expected, ListTablesResponse actual)
    {
        // there was a bug in these tests before - the ExampleMetadataHandler doesn't actually sort the tables if it has no pagination,
        // but the tests implied they were supposed to by comparing the objects. However, the equals method defined in the old Response class
        // just checked if the two lists had all the same values (unordered). Because the equals method is now more refined for the generated
        // protobuf class, we have to manually do the same checks.
        assertTrue(CollectionsUtils.equals(expected.getTablesList(), actual.getTablesList()));
        assertEquals(expected.getCatalogName(), actual.getCatalogName());
        assertEquals(expected.getNextToken(), actual.getNextToken());
    }

    @Test
    public void doGetTable()
    {
        logger.info("doGetTable - enter");
        GetTableRequest req = GetTableRequest.newBuilder()
            .setIdentity(IdentityUtil.fakeIdentity())
            .setQueryId("queryId")
            .setCatalogName("default")
            .setTableName(
                com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                    .setSchemaName("custom_source")
                    .setTableName("fake_table")
                    .build()
            ).build();
        // ObjectMapperUtil.assertSerialization(req);
        GetTableResponse res = metadataHandler.doGetTable(allocator, req);
        // ObjectMapperUtil.assertSerialization(res);
        Schema arrowSchema = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema());
        assertTrue(arrowSchema.getFields().size() > 0);
        assertTrue(arrowSchema.getCustomMetadata().size() > 0);
        logger.info("doGetTable - {}", res);
        logger.info("doGetTable - exit");
    }

    @Test(expected = LambdaFunctionException.class)
    public void doGetTableFail()
    {
        try {
            logger.info("doGetTableFail - enter");
            GetTableRequest req = GetTableRequest.newBuilder()
                .setIdentity(IdentityUtil.fakeIdentity())
                .setQueryId("queryId")
                .setCatalogName("default")
                .setTableName(
                    com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                        .setSchemaName("lambda")
                        .setTableName("fake")
                        .build()
                ).build();

            metadataHandler.doGetTable(allocator, req);
        }
        catch (Exception ex) {
            logger.info("doGetTableFail: ", ex);
            throw new LambdaFunctionException(ex.getMessage(), false, "repackaged");
        }
    }

    /**
     * 200,000,000 million partitions pruned down to 38,000 and transmitted in 25 seconds
     *
     * @throws Exception
     */
    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("day");
        partitionCols.add("month");
        partitionCols.add("year");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("day", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 20)), false));

        constraintsMap.put("month", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 2)), false));

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1900)), false));

        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;
        try {
            req = GetTableLayoutRequest.newBuilder()
                .setIdentity(IdentityUtil.fakeIdentity())
                .setQueryId("queryId")
                .setCatalogName("default")
                .setTableName(ProtobufMessageConverter.toTableName(new TableName("schema1", "table1")))
                .setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(tableSchema))
                .addAllPartitionCols(partitionCols)
                .build();

            // ObjectMapperUtil.assertSerialization(req);

            res = metadataHandler.doGetTableLayout(allocator, req);
            // ObjectMapperUtil.assertSerialization(res);

            logger.info("doGetTableLayout - {}", res);
            Block partitions = ProtobufMessageConverter.fromProtoBlock(allocator, res.getPartitions());
            for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
        }
        finally {
            try {
                ProtobufMessageConverter.fromProtoConstraints(allocator, req.getConstraints()).close();
                ProtobufMessageConverter.fromProtoBlock(allocator, res.getPartitions()).close();
            }
            catch (Exception ex) {
                logger.error("doGetTableLayout: ", ex);
            }
        }

        logger.info("doGetTableLayout - exit");
    }

    /**
     * The goal of this test is to test happy case for getting splits and also to exercise the continuation token
     * logic specifically.
     * @throws InvalidProtocolBufferException
     */
    @Test
    public void doGetSplits() throws InvalidProtocolBufferException
    {
        logger.info("doGetSplits: enter");

        String yearCol = "year";
        String monthCol = "month";
        String dayCol = "day";

        //This is the schema that ExampleMetadataHandler has layed out for a 'Partition' so we need to populate this
        //minimal set of info here.
        Schema schema = SchemaBuilder.newBuilder()
                .addField(yearCol, new ArrowType.Int(16, false))
                .addField(monthCol, new ArrowType.Int(16, false))
                .addField(dayCol, new ArrowType.Int(16, false))
                .addField(ExampleMetadataHandler.PARTITION_LOCATION, new ArrowType.Utf8())
                .addField(ExampleMetadataHandler.SERDE, new ArrowType.Utf8())
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(yearCol);
        partitionCols.add(monthCol);
        partitionCols.add(dayCol);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(dayCol, SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 20)), false));

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 100;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(yearCol), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector(monthCol), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector(dayCol), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector(ExampleMetadataHandler.PARTITION_LOCATION), i, String.valueOf(i));
            BlockUtils.setValue(partitions.getFieldVector(ExampleMetadataHandler.SERDE), i, "TextInputType");
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        int numContinuations = 0;
        boolean hasContinuationToken = false;
        do {
            // TODO - figure out right way to copy a Message

            GetSplitsRequest.Builder reqBuilder = GetSplitsRequest.newBuilder()
            .setIdentity(
                com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity.newBuilder()
                    .setPrincipal("principal")
                    .setAccount("123")
                    .setArn("arn")
                    .build())
            .setQueryId("queryId")
            .setCatalogName("catalog_name")
            .setTableName(
                com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                .setSchemaName("schema")
                .setTableName("table_name")
                .build()
            )
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .setConstraints(
                com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints.newBuilder()
                .putAllSummary(ProtobufMessageConverter.toProtoSummary(constraintsMap))
                .build()
            )
            .addAllPartitionCols(partitionCols);

            if (continuationToken != null)
            {
                reqBuilder.setContinuationToken(continuationToken);
            }
            GetSplitsRequest req = reqBuilder.build();
            

            logger.info("doGetSplits: req[{}]", req);
            metadataHandler.setEncryption(numContinuations % 2 == 0);
            logger.info("doGetSplits: Toggle encryption " + (numContinuations % 2 == 0));

            GetSplitsResponse response = metadataHandler.doGetSplits(allocator, req);
            // assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - numSplits[{}] - maxSplits[{}]",
                    new Object[] {continuationToken, response.getSplitsList().size(), MAX_SPLITS_PER_REQUEST});

            for (com.amazonaws.athena.connector.lambda.proto.domain.Split nextSplit : response.getSplitsList()) {
                if (numContinuations % 2 == 0) {
                    assertNotNull(nextSplit.getEncryptionKey());
                }
                else {
                    assertEquals(com.google.protobuf.ByteString.EMPTY, nextSplit.getEncryptionKey().getKey());
                    assertEquals(com.google.protobuf.ByteString.EMPTY, nextSplit.getEncryptionKey().getNonce());
                }
                assertNotNull(nextSplit.getPropertiesMap().get(SplitProperties.LOCATION.getId()));
                assertNotNull(nextSplit.getPropertiesMap().get(SplitProperties.SERDE.getId()));
                assertNotNull(nextSplit.getPropertiesMap().get(SplitProperties.SPLIT_PART.getId()));
            }

            assertTrue("Continuation criteria violated", (response.getSplitsList().size() == MAX_SPLITS_PER_REQUEST &&
                    response.hasContinuationToken()) || response.getSplitsList().size() < MAX_SPLITS_PER_REQUEST);

            if (response.hasContinuationToken()) {
                numContinuations++;
            }

            hasContinuationToken = response.hasContinuationToken();

            logger.info("Logging response from protobuf. {}", ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(response));
            logger.info("done logging response");
        }
        while (hasContinuationToken);

        assertTrue(numContinuations > 0);

        logger.info("doGetSplits: exit");
    }
}
