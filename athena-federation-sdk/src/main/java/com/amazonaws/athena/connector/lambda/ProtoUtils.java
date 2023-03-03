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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.v3.BlockSerDeV3;
import com.google.protobuf.ByteString;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoUtils 
{
    
    private static final String ALL_OR_NONE_VALUE_SET_TYPE = "@AllOrNoneValueSet";
    private static final String EQUATABLE_VALUE_SET_TYPE = "@EquatableValueSet";
    private static final String SORTED_RANGE_SET_TYPE = "@SortedRangeSet";

    private ProtoUtils()
    {
        // do nothing
    }

    // TODO: Add all supported types
    public static com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage toProtoArrowType(ArrowType arrowType)
    {
        com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage.Builder arrowTypeMessageBuilder = com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage.newBuilder();
       
        // BaseSerializer::writeType does the same thing as here 
        arrowTypeMessageBuilder.setType(arrowType.getClass().getSimpleName());

        //now, we have to write whatever fields the ArrowType constructor would take in based on the subtype.
        // Refer to ArrowTypeSerDe and the subclasses' doTypedSerialize implementation
        if (arrowType instanceof ArrowType.FloatingPoint) {
            var fp = (ArrowType.FloatingPoint) arrowType;
            arrowTypeMessageBuilder.setPrecision(com.amazonaws.athena.connector.lambda.proto.arrowtype.Precision.valueOf(fp.getPrecision().name()));
        }

        return arrowTypeMessageBuilder.build();
    }

    // TODO: Add all supported types
    public static ArrowType fromProtoArrowType(com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage arrowTypeMessage)
    {
        var className = arrowTypeMessage.getType();
        
        // we can use reflection for the classes that have no-arg constructors, but 
        // there's not a good way to do it for classes with arg constructors.
        if (className.equals("FloatingPoint")) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.valueOf(arrowTypeMessage.getPrecision().name()));
        }
        return null;
    }

    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker toProtoMarker(Marker marker)
    {
        return com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker.newBuilder()
            .setValueBlock(ProtoUtils.toProtoBlock(marker.getValueBlock()))
            .setBound(com.amazonaws.athena.connector.lambda.proto.domain.predicate.Bound.valueOf(marker.getBound().name()))
            .setNullValue(marker.isNullValue())
            .build();
    }

    public static Marker fromProtoMarker(BlockAllocator blockAllocator, com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker protoMarker)
    {
        return new Marker(
            ProtoUtils.fromProtoBlock(blockAllocator, protoMarker.getValueBlock()),
            Marker.Bound.valueOf(protoMarker.getBound().name()),
            protoMarker.getNullValue()
        );
    }

    public static List<com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range> toProtoRanges(List<Range> ranges)
    {
        return ranges.stream()
            .map(range -> com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range.newBuilder()
                    .setLow(toProtoMarker(range.getLow()))
                    .setHigh(toProtoMarker(range.getHigh()))
                    .build())
            .collect(Collectors.toList());
    }

    public static List<Range> fromProtoRanges(BlockAllocator blockAllocator, List<com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range> protoRanges)
    {
        return protoRanges.stream()
            .map(range -> new Range(
                fromProtoMarker(blockAllocator, range.getLow()),
                fromProtoMarker(blockAllocator, range.getHigh())
            ))
            .collect(Collectors.toList());
    }

    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet toProtoValueSet(ValueSet valueSet)
    {
        com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet.Builder outBuilder =
             com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet.newBuilder();

        if (valueSet instanceof AllOrNoneValueSet) {
            AllOrNoneValueSet allOrNone = (AllOrNoneValueSet) valueSet;
            return outBuilder
                .setTypeField(ALL_OR_NONE_VALUE_SET_TYPE)
                .setArrowTypeMessage(ProtoUtils.toProtoArrowType(allOrNone.getType()))
                .setAll(allOrNone.isAll())
                .setNullAllowed(allOrNone.isNullAllowed())
                .build();
        } 
        else if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet eqValueSet = (EquatableValueSet) valueSet;
            return outBuilder
                .setTypeField(EQUATABLE_VALUE_SET_TYPE)
                .setValueBlock(ProtoUtils.toProtoBlock(eqValueSet.getValueBlock()))
                .setWhiteList(eqValueSet.isWhiteList())
                .setNullAllowed(eqValueSet.isNullAllowed())
                .build();
        } 
        else if (valueSet instanceof SortedRangeSet) {
            SortedRangeSet sortedRangeSet = (SortedRangeSet) valueSet;
            return outBuilder
                .setTypeField(SORTED_RANGE_SET_TYPE)
                .setArrowTypeMessage(ProtoUtils.toProtoArrowType(sortedRangeSet.getType()))
                .addAllRanges(toProtoRanges(sortedRangeSet.getOrderedRanges()))
                .setNullAllowed(sortedRangeSet.isNullAllowed())
                .build();
        }
        else {
            throw new IllegalArgumentException("Unexpected subtype of ValueSet encountered.");
        }
    }

    public static ValueSet fromProtoValueSet(BlockAllocator blockAllocator, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet protoValueSet)
    {
        String type = protoValueSet.getTypeField();
        if (type.equals(ALL_OR_NONE_VALUE_SET_TYPE)) {
            return new AllOrNoneValueSet(
                fromProtoArrowType(protoValueSet.getArrowTypeMessage()),
                protoValueSet.getAll(),
                protoValueSet.getNullAllowed()
            );
        }
        else if (type.equals(EQUATABLE_VALUE_SET_TYPE)) {
            return new EquatableValueSet(
                fromProtoBlock(blockAllocator, protoValueSet.getValueBlock()),
                protoValueSet.getWhiteList(),
                protoValueSet.getNullAllowed()
            );
        }
        else if (type.equals(SORTED_RANGE_SET_TYPE)) {
            // this static copy is how the deserializer builds the Sorted Range Set as well
            return SortedRangeSet.copyOf(
                fromProtoArrowType(protoValueSet.getArrowTypeMessage()),
                fromProtoRanges(blockAllocator, protoValueSet.getRangesList()),
                protoValueSet.getNullAllowed()
            );
        }
        else {
            throw new IllegalArgumentException("Unexpected value set received over the wire - type " + type + " is not recognized.");
        }
    }

    public static Map<String, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet> toProtoSummary(Map<String, ValueSet> summaryMap)
    {
        return summaryMap.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> toProtoValueSet(e.getValue())
            ));
    }

    public static Map<String, ValueSet> fromProtoSummary(BlockAllocator blockAllocator, Map<String, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet> protoSummaryMap)
    {
        return protoSummaryMap.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> fromProtoValueSet(blockAllocator, e.getValue())
            ));
    }

    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints toProtoConstraints(Constraints constraints)
    {
        return com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints.newBuilder()
            .putAllSummary(toProtoSummary(constraints.getSummary()))
            .build();
    }

    public static Constraints fromProtoConstraints(BlockAllocator blockAllocator, com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints protoConstraints)
    {
        return new Constraints(fromProtoSummary(blockAllocator, protoConstraints.getSummaryMap()));
    }
    
    public static com.amazonaws.athena.connector.lambda.proto.data.Block toProtoBlock(Block block)
    {
        ByteArrayOutputStream outSchema = new ByteArrayOutputStream();
        ByteArrayOutputStream outRecords = new ByteArrayOutputStream();
        try {
            // need to convert schema to byte array. Taken from SchemaSerDeV3.java
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outSchema)), block.getSchema());

            try (var batch = block.getRecordBatch()) {
                if (batch.getLength() > 0) { // if we don't do this conditionally, it writes a non-empty string, which breaks our current serde
                    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outRecords)), batch);
                }
            }
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
        if (protoRecords.length > 0) {
            var arrowBatch = BlockSerDeV3.Deserializer.deserializeRecordBatch(allocator, protoRecords);
            block.loadRecordBatch(arrowBatch);
        }

        return block;
    }
}
