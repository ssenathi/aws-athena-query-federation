/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.athena.connector.lambda.proto.metadata.*;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public class SaphanaMetadataHandlerTest
        extends TestBase
{
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SaphanaConstants.SAPHANA_NAME,
            "saphana://jdbc:sap://hostname/user=A&password=B");
    private SaphanaMetadataHandler saphanaMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private BlockAllocator blockAllocator;
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField("PART_ID", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();


    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        this.saphanaMetadataHandler = new SaphanaMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of("spill_bucket", "asdf_spill_bucket_loc"));
        this.federatedIdentity = FederatedIdentity.newBuilder().build();
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.saphanaMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList("PART_ID")); //partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionCols(partitionCols).build();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"PART_ID"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);


        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()), i));
        }
        Assert.assertEquals(expectedValues, Arrays.asList("[PART_ID : p0]", "[PART_ID : p1]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getSchemaName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionCols(partitionCols).build();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"PART_ID"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()), i));
        }
        Assert.assertEquals(expectedValues, Collections.singletonList("[PART_ID : 0]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionCols(partitionCols).build();

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        SaphanaMetadataHandler saphanaMetadataHandler = new SaphanaMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        saphanaMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        String[] columns = {SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionCols(partitionCols).build();

        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setPartitions(ProtobufMessageConverter.toProtoBlock(ProtobufMessageConverter.toProtoBlock(getTableLayoutResponse.getPartitions()))).addAllPartitionCols(new ArrayList<>(partitionCols)).setConstraints(ProtobufMessageConverter.toProtoConstraints(ProtobufMessageConverter.toProtoConstraints(constraints))).setContinuationToken($8).build();
        GetSplitsResponse getSplitsResponse = this.saphanaMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, "p0"));
        expectedSplits.add(Collections.singletonMap(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplitsList().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplitsList().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsContinuation()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionCols(partitionCols).build();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"PART_ID"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setPartitions(getTableLayoutResponse.getPartitions()).addAllPartitionCols(new ArrayList<>(partitionCols)).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setContinuationToken("1")).build();
        GetSplitsResponse getSplitsResponse = this.saphanaMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap("PART_ID", "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplitsList().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplitsList().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}, {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());

        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();
        TableName inputTableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        GetTableResponse getTableResponse = this.saphanaMetadataHandler.doGetTable(
                this.blockAllocator, GetTableRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").setTableName(inputTableName).build());
        Assert.assertEquals(expected, ProtobufMessageConverter.fromProtoSchema(blockAllocator, getTableResponse.getSchema()));
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void testFindTableNameFromQueryHint()
            throws Exception
    {
        TableName inputTableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable@schemacase=upper&tablecase=upper").build();
        TableName tableName = saphanaMetadataHandler.findTableNameFromQueryHint(inputTableName);
        Assert.assertEquals(TableName.newBuilder().setSchemaName("TESTSCHEMA", "TESTTABLE")).setTableName(tableName).build();

        TableName inputTableName1 = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable@schemacase=upper&tablecase=lower").build();
        TableName tableName1 = saphanaMetadataHandler.findTableNameFromQueryHint(inputTableName1);
        Assert.assertEquals(TableName.newBuilder().setSchemaName("TESTSCHEMA", "testtable")).setTableName(tableName1).build();

        TableName inputTableName2 = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable@schemacase=lower&tablecase=lower").build();
        TableName tableName2 = saphanaMetadataHandler.findTableNameFromQueryHint(inputTableName2);
        Assert.assertEquals(TableName.newBuilder().setSchemaName("testschema", "testtable")).setTableName(tableName2).build();

        TableName inputTableName3 = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable@schemacase=lower&tablecase=upper").build();
        TableName tableName3 = saphanaMetadataHandler.findTableNameFromQueryHint(inputTableName3);
        Assert.assertEquals(TableName.newBuilder().setSchemaName("testschema", "TESTTABLE")).setTableName(tableName3).build();

    }

    @Test
    public void doGetSplitsForView()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();

        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SaphanaConstants.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        ResultSet viewCheckqueryResultSet = mockResultSet(new String[] {"VIEW_NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"COVID19"}}, new AtomicInteger(-1));
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenReturn(viewCheckqueryResultSet);

        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());

        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionCols(partitionCols).build();

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setPartitions(ProtobufMessageConverter.toProtoBlock(ProtobufMessageConverter.toProtoBlock(getTableLayoutResponse.getPartitions()))).addAllPartitionCols(new ArrayList<>(partitionCols)).setConstraints(ProtobufMessageConverter.toProtoConstraints(ProtobufMessageConverter.toProtoConstraints(constraints))).setContinuationToken($8).build();
        GetSplitsResponse getSplitsResponse = this.saphanaMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, "0"));

        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplitsList().size());

        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplitsList().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.saphanaMetadataHandler.doGetTable(this.blockAllocator, GetTableRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").setTableName(inputTableName).build());
    }

    @Test (expected = RuntimeException.class)
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        this.saphanaMetadataHandler.doGetTable(this.blockAllocator, GetTableRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").setTableName(inputTableName).build());
    }
}
