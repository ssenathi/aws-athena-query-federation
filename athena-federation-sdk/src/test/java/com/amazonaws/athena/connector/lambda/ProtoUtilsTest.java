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

import static org.junit.Assert.*;

import java.util.List;

import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;

public class ProtoUtilsTest {

    private BlockAllocator blockAllocator;

    @Before
    public void setup()
    {
        blockAllocator = new BlockAllocatorImpl();
    }

    @After
    public void cleanup()
    {
        // blockAllocator.close();
    }

    @Test
    public void testToAndFromProtoBlock() throws Exception
    {

        Block block = BlockUtils.newBlock(blockAllocator, "col1", Types.MinorType.INT.getType(), List.of(1, 2, 3));
        com.amazonaws.athena.connector.lambda.proto.data.Block protoBlock = ProtoUtils.toProtoBlock(block);
        Block rewritten = ProtoUtils.fromProtoBlock(blockAllocator, protoBlock);

        // the block equals method doesn't test allocator id
        assertEquals(block.getAllocatorId(), rewritten.getAllocatorId());
        assertEquals(block, rewritten);
    }

    @Test
    public void testFromProtoBlock()
    {

    }
}
