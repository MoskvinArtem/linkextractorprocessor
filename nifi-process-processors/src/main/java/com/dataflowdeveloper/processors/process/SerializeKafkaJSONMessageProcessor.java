package com.dataflowdeveloper.processors.process;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"kafka", "json", "serialization"})
@SupportsBatching
@CapabilityDescription("TODO Description of the Processor")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class SerializeKafkaJSONMessageProcessor extends AbstractProcessor {

    public static final String MESSAGE_DESTINATION_NAME = "Message Destination";
    public static final String MESSAGE_FLOWFILE_ATTRIBUTE_NAME = "Flowfile Attribute Name";
    public static final String MESSAGE_FLOWFILE_ATTRIBUTE = "kafka-string-json-message";

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private ObjectMapper mapper;
    private volatile boolean destinationContent;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully extracted Links.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to extract links.")
            .build();

    public static final PropertyDescriptor MESSAGE_DESTINATION_PROPERTY = new PropertyDescriptor
            .Builder().name(MESSAGE_DESTINATION_NAME)
            .description("Flag")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor MESSAGE_FLOWFILE_ATTRIBUTE_NAME_PROPERTY = new PropertyDescriptor
            .Builder().name(MESSAGE_FLOWFILE_ATTRIBUTE_NAME)
            .description("Attribute name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(MESSAGE_FLOWFILE_ATTRIBUTE)
            .build();


    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MESSAGE_DESTINATION_PROPERTY);
        properties.add(MESSAGE_FLOWFILE_ATTRIBUTE_NAME_PROPERTY);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        mapper = new ObjectMapper();
        destinationContent = DESTINATION_CONTENT.equals(context.getProperty(MESSAGE_DESTINATION_PROPERTY).getValue());
    }



    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final AtomicReference<String> jsonStringRef = new AtomicReference<>();
        FlowFile atFlowfile, conFlowfile;

        try {
            session.read(original, (in) -> {
                try (BufferedInputStream bufferedIn = new BufferedInputStream(in)) {
                    String message = IOUtils.toString(bufferedIn, "UTF-8");
                    JsonNode jsonNode;
                    try {
                        jsonNode = mapper.readTree(message);
                    } catch (Exception e) {
                        jsonNode = mapper.createObjectNode();
                        getLogger().error("Failed one", e);
                        session.putAttribute(original, "error_attribute", e.getMessage());
                        e.printStackTrace();
                    }
                    jsonStringRef.set(mapper.writeValueAsString(jsonNode));
                }
            });

            if (destinationContent) {
                conFlowfile = session.write(original, out -> out.write(jsonStringRef.get().getBytes(StandardCharsets.UTF_8)));
                session.transfer(conFlowfile, REL_SUCCESS);
            } else {
                atFlowfile = session.putAttribute(
                        original,
                        context.getProperty(MESSAGE_FLOWFILE_ATTRIBUTE_NAME_PROPERTY).getValue(),
                        jsonStringRef.get()
                );
                session.transfer(atFlowfile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error("Failed to convert message to JSON. Routing to failure.", e);
            session.transfer(original, REL_FAILURE);
        }
    }
}