/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;

import java.util.List;
import java.util.Map;

/**
 * Handles metadata requests for the Athena AWS CMDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Maps AWS Resources to SQL tables using a set of TableProviders constructed from a TableProviderFactory.
 * 2. This class is largely a mux that delegates requests to the appropriate TableProvider based on the
 * requested TableName.
 * 3. Provides a schema and table list by scanning all loaded TableProviders.
 */
public class AwsCmdbMetadataHandler
        extends MetadataHandler
{
    private static final String SOURCE_TYPE = "cmdb";
    //Map of schema name to list of TableNames generated by scanning all loaded TableProviders.
    private Map<String, List<TableName>> schemas;
    //Map of available fully qualified TableNames to their respective TableProviders.
    private Map<TableName, TableProvider> tableProviders;

    public AwsCmdbMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        TableProviderFactory tableProviderFactory = new TableProviderFactory(configOptions);
        schemas = tableProviderFactory.getSchemas();
        tableProviders = tableProviderFactory.getTableProviders();
    }

    @VisibleForTesting
    protected AwsCmdbMetadataHandler(
        TableProviderFactory tableProviderFactory,
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        schemas = tableProviderFactory.getSchemas();
        tableProviders = tableProviderFactory.getTableProviders();
    }

    /**
     * Returns the list of supported schemas discovered from the loaded TableProvider scan.
     *
     * @see MetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas.keySet());
    }

    /**
     * Returns the list of supported tables on the requested schema discovered from the loaded TableProvider scan.
     *
     * @see MetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        return new ListTablesResponse(listTablesRequest.getCatalogName(),
                schemas.get(listTablesRequest.getSchemaName()), null);
    }

    /**
     * Delegates to the TableProvider that is registered for the requested table.
     *
     * @see MetadataHandler
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        TableProvider tableProvider = tableProviders.get(getTableRequest.getTableName());
        if (tableProvider == null) {
            throw new RuntimeException("Unknown table " + getTableRequest.getTableName());
        }
        return tableProvider.getTable(blockAllocator, getTableRequest);
    }

    /**
     * Delegates to the TableProvider that is registered for the requested table.
     *
     * @see MetadataHandler
     */
    @Override
    public void enhancePartitionSchema(BlockAllocator allocator, SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        TableProvider tableProvider = tableProviders.get(request.getTableName());
        if (tableProvider == null) {
            throw new RuntimeException("Unknown table " + request.getTableName());
        }
        tableProvider.enhancePartitionSchema(partitionSchemaBuilder, request);
    }

    /**
     * Delegates to the TableProvider that is registered for the requested table.
     *
     * @see MetadataHandler
     */
    @Override
    public void getPartitions(BlockAllocator allocator, BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        TableProvider tableProvider = tableProviders.get(request.getTableName());
        if (tableProvider == null) {
            throw new RuntimeException("Unknown table " + request.getTableName());
        }
        tableProvider.getPartitions(blockWriter, request);
    }

    /**
     * Delegates to the TableProvider that is registered for the requested table.
     *
     * @see MetadataHandler
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        TableProvider tableProvider = tableProviders.get(getSplitsRequest.getTableName());
        if (tableProvider == null) {
            throw new RuntimeException("Unknown table " + getSplitsRequest.getTableName());
        }

        //Every split needs a unique spill location.
        SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
        EncryptionKey encryptionKey = makeEncryptionKey();
        Split split = Split.newBuilder(spillLocation, encryptionKey).build();
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), split);
    }
}
