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
package com.amazonaws.athena.connector.lambda;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.v3.BlockSerDeV3;
import com.google.protobuf.ByteString;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoUtils 
{
    private ProtoUtils()
    {
        // do nothing
    }

    // TODO: FILL OUT
    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet toProtoValueSet(ValueSet valueSet)
    {
        com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet.Builder outBuilder =
             com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet.newBuilder();

        if (valueSet instanceof AllOrNoneValueSet) {
            // TBD
        } 
        else if (valueSet instanceof EquatableValueSet) {
            // TBD
        } 
        else if (valueSet instanceof SortedRangeSet) {
            // TBD
        }

        return outBuilder.build();
    }

    public static Map<String, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet> toProtoSummary(Map<String, ValueSet> summaryMap)
    {
        return summaryMap.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> toProtoValueSet(e.getValue())
            ));
    }
    
    public static com.amazonaws.athena.connector.lambda.proto.data.Block toProtoBlock(Block block)
    {
        ByteArrayOutputStream outSchema = new ByteArrayOutputStream();
        ByteArrayOutputStream outRecords = new ByteArrayOutputStream();
        try {
            // need to convert schema to byte array. Taken from SchemaSerDeV3.java
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outSchema)), block.getSchema());
            var batch = block.getRecordBatch();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outRecords)), batch);
            batch.close(); // we leak memory unless we close this
            
        }
        catch (IOException ie) {
            // whatever for now
        }
        return com.amazonaws.athena.connector.lambda.proto.data.Block.newBuilder()
            .setAId(block.getAllocatorId())
            .setSchema(ByteString.copyFrom(outSchema.toByteArray()))
            .setRecords(ByteString.copyFrom(outRecords.toByteArray()))
            .build();
    }

    public static Block fromProtoBlock(BlockAllocator allocator, com.amazonaws.athena.connector.lambda.proto.data.Block protoBlock)
    {
        ByteArrayInputStream in = new ByteArrayInputStream(protoBlock.getSchema().toByteArray());
        Schema schema = null;
        try {
            schema = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
        }
        catch (IOException ie) {
            // ignore for now
        }

        var block = allocator.createBlock(schema);

        var protoRecords = protoBlock.getRecords().toByteArray();
        var arrowBatch = BlockSerDeV3.Deserializer.deserializeRecordBatch(allocator, protoRecords);
        System.out.println("DESERIALIZED INTO ARROW BATCH.");
        System.out.println(arrowBatch);
        block.loadRecordBatch(arrowBatch);

        return block;
    }
}
