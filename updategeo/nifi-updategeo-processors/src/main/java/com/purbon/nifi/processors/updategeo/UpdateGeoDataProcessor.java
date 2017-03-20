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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.maxmind.geoip2.DatabaseReader;

import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class UpdateGeoDataProcessor extends AbstractProcessor {

    public static final PropertyDescriptor IP_FIELD = new PropertyDescriptor
            .Builder().name("IP_FIELD")
            .displayName("IP location")
            .description("The expression to select the IP")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor
            .Builder().name("DATA_FORMAT")
            .displayName("Data format")
            .description("Format the data is coming into this processor")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("SUCCESS_RELATIONSHIP")
            .description("Success relationship")
            .build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("FAILURE_RELATIONSHIP")
            .description("Failure relationship")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private ObjectMapper mapper;
    
    private File maxmindDB;
    DatabaseReader reader;
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(IP_FIELD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
        
        String dbFilePath = getClass().getResource("maxmind/GeoLite2-City.mmdb").getFile();
        File maxmindDB = new File(dbFilePath);
        try {
			 reader = new DatabaseReader.Builder(maxmindDB).build();
		} catch (IOException e) {
			context.getNodeTypeProvider();
		}

        mapper = new ObjectMapper();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final InputStream in = session.read(flowFile);
		try {
			String data = IOUtils.toString(in, StandardCharsets.UTF_8);
			Map<String, Object> map = asJSON(data);
			in.close();
			
			String ipFieldExpression = context.getProperty(IP_FIELD).getValue();
			String ipFieldString = (String)JsonPath.read(data, ipFieldExpression);
			Map<String, Object> map2 = new HashMap<String, Object>();
			map2.put("city", ipFieldString);
			
			
			
			map.put("geolocation", map2);
			
			flowFile = session.write(flowFile, new OutputStreamCallback() {
				@Override
				public void process(OutputStream out) throws IOException {
					out.write(mapper.writeValueAsString(map).getBytes());
				}
			});
	        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
		} catch (IOException e) {
			e.printStackTrace();
			session.penalize(flowFile);
			session.transfer(flowFile, FAILURE_RELATIONSHIP);
		}
        
    }
    
    @SuppressWarnings("unchecked")
	private Map<String, Object> asJSON(String jsonString) throws JsonParseException, JsonMappingException, IOException {
    	TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>(){};
    	return (Map<String, Object>)mapper.readValue(jsonString, typeRef);
    }
}
