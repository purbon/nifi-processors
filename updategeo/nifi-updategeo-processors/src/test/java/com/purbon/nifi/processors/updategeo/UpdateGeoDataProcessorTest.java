/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.purbon.nifi.processors.updategeo;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

public class UpdateGeoDataProcessorTest {

    private TestRunner testRunner;
    private ObjectMapper mapper;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(UpdateGeoDataProcessor.class);
        mapper = new ObjectMapper();
    }

    @Test
    public void testProcessor() throws IOException {
    	InputStream in = buildJSONPayload();
    	testRunner.enqueue(in);
    	testRunner.setProperty(UpdateGeoDataProcessor.IP_FIELD, "$.ip");
    	testRunner.run();
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(UpdateGeoDataProcessor.SUCCESS_RELATIONSHIP);
    	assertEquals(1, flowFiles.size());
    	
    	MockFlowFile flowFile = flowFiles.get(0);
    	String jsonString = new String(flowFile.toByteArray());
    
    	String city = JsonPath.read(jsonString, "$.geolocation.city");
    	assertEquals("MountainView", city);
    }
    
    
    
    private InputStream buildJSONPayload() throws IOException {
    	Map<String, Object> payload = new HashMap<String, Object>();
    	payload.put("ip", "8.8.8.8");
    	return new ByteArrayInputStream(mapper.writeValueAsString(payload).getBytes());
    }

}
