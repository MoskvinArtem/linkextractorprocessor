package com.dataflowdeveloper.processors.process;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"kafka", "json", "serialization"})
@SupportsBatching
@CapabilityDescription("TODO Description of the Processor")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class SerializeKafkaJSONMessageProcessor extends AbstractProcessor {

    private Set<Relationship> relationships;
    private ObjectMapper mapper;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully extracted Links.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to extract links.")
            .build();



    @Override
    protected void init(ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        mapper = new ObjectMapper();
        return;
    }



    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AtomicReference<String> jsonStringRef = new AtomicReference<>();

        try {
            // Read the incoming message
            session.read(flowFile, in -> {
                try (BufferedInputStream bufferedIn = new BufferedInputStream(in)) {
                    String message = IOUtils.toString(bufferedIn, "UTF-8");

                    // Convert the message to JSON
                    JsonNode jsonNode;


                    try {
                        jsonNode = mapper.readTree(message); // Check if already valid JSON
                    } catch (IOException e) {
                        jsonNode = mapper.createObjectNode();
                        e.printStackTrace();
                    }

                    jsonStringRef.set(mapper.writeValueAsString(jsonNode));
                }
            });

            // Write the JSON back to the FlowFile
            flowFile = session.write(flowFile, out -> out.write(jsonStringRef.get().getBytes("UTF-8")));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to convert message to JSON. Routing to failure.", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}