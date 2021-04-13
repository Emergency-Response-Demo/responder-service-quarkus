package com.redhat.erdemo.responder.service;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.util.Arrays;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.ce.impl.DefaultOutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class EventPublisherTest {

    @Inject
    EventPublisher eventPublisher;

    @Inject @Any
    InMemoryConnector connector;

    @BeforeEach
    void init() {
        connector.sink("responder-event").clear();
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testResponderCreated() {

        InMemorySink<String> results = connector.sink("responder-event");

        Responder responder = new Responder.Builder("responder123").name("John Doe").phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345")).longitude(new BigDecimal("-77.98765")).boatCapacity(10)
                .medicalKit(true).available(true).person(false).enrolled(true).build();

        eventPublisher.responderCreated(responder);

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("created", 1));
        assertThat(value, jsonPartEquals("responders[0].id", "responder123"));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, notNullValue());
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("RespondersCreatedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
        assertThat(outgoingCloudEventMetadata.getExtension("incidentid").isEmpty(), is(true));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testRespondersCreated() {

        InMemorySink<String> results = connector.sink("responder-event");

        Responder responder1 = new Responder.Builder("responder123").name("John Doe").phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345")).longitude(new BigDecimal("-77.98765")).boatCapacity(10)
                .medicalKit(true).available(true).person(false).enrolled(true).build();

        Responder responder2 = new Responder.Builder("responder456").name("John Doe").phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345")).longitude(new BigDecimal("-77.98765")).boatCapacity(10)
                .medicalKit(true).available(true).person(false).enrolled(true).build();

        Responder responder3 = new Responder.Builder("responder789").name("John Doe").phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345")).longitude(new BigDecimal("-77.98765")).boatCapacity(10)
                .medicalKit(true).available(true).person(false).enrolled(true).build();

        eventPublisher.respondersCreated(Arrays.asList(responder1, responder2, responder3));

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("created", 3));
        assertThat(value, jsonPartEquals("responders[0].id", "responder123"));
        assertThat(value, jsonPartEquals("responders[1].id", "responder456"));
        assertThat(value, jsonPartEquals("responders[2].id", "responder789"));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, Matchers.notNullValue());
        assertThat(outgoingKafkaRecordMetadata, Matchers.notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, notNullValue());
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("RespondersCreatedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
        assertThat(outgoingCloudEventMetadata.getExtension("incidentid").isEmpty(), is(true));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testRespondersDeleted() {

        InMemorySink<String> results = connector.sink("responder-event");

        eventPublisher.respondersDeleted(Arrays.asList("1", "2", "3"));

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("deleted", 3));
        assertThat(value, jsonPartEquals("responders[0]", "\"1\""));
        assertThat(value, jsonPartEquals("responders[1]", "\"2\""));
        assertThat(value, jsonPartEquals("responders[2]", "\"3\""));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, Matchers.notNullValue());
        assertThat(outgoingKafkaRecordMetadata, Matchers.notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, notNullValue());
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("RespondersDeletedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
        assertThat(outgoingCloudEventMetadata.getExtension("incidentid").isEmpty(), is(true));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testResponderSetUnavailable() {

        InMemorySink<String> results = connector.sink("responder-event");

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        eventPublisher.responderSetUnavailable(ImmutableTriple.of(true, "message", responder1), "incident123");

        Message<String> message = results.received().get(0);
        assertThat(results.received().size(), equalTo(1));
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("status", "success"));
        assertThat(value, jsonPartEquals("statusMessage", "message"));
        assertThat(value, jsonNodePresent("responder"));
        assertThat(value, jsonPartEquals("responder.id", "\"1\""));
        assertThat(value, jsonPartEquals("responder.name", "John Doe"));
        assertThat(value, jsonPartEquals("responder.phoneNumber", "111-222-333"));
        assertThat(value, jsonPartEquals("responder.latitude", 30.12345));
        assertThat(value, jsonPartEquals("responder.longitude", -70.98765));
        assertThat(value, jsonPartEquals("responder.boatCapacity", 3));
        assertThat(value, jsonPartEquals("responder.medicalKit", true));
        assertThat(value, jsonPartEquals("responder.available", true));
        assertThat(value, jsonPartEquals("responder.enrolled", true));
        assertThat(value, jsonPartEquals("responder.person", false));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, equalTo("1"));
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("ResponderSetUnavailableEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
        assertThat(outgoingCloudEventMetadata.getExtension("incidentid").isPresent(), is(true));
        assertThat(outgoingCloudEventMetadata.getExtension("incidentid").get(), equalTo("incident123"));
    }

    @Test
    void testResponderUpdated() {

        InMemorySink<String> results = connector.sink("responder-event");

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        eventPublisher.responderUpdated(responder1);

        Message<String> message = results.received().get(0);
        assertThat(results.received().size(), equalTo(1));
        String value = message.getPayload();
        assertThat(value, jsonNodePresent("responder"));
        assertThat(value, jsonPartEquals("responder.id", "\"1\""));
        assertThat(value, jsonPartEquals("responder.name", "John Doe"));
        assertThat(value, jsonPartEquals("responder.phoneNumber", "111-222-333"));
        assertThat(value, jsonPartEquals("responder.latitude", 30.12345));
        assertThat(value, jsonPartEquals("responder.longitude", -70.98765));
        assertThat(value, jsonPartEquals("responder.boatCapacity", 3));
        assertThat(value, jsonPartEquals("responder.medicalKit", true));
        assertThat(value, jsonPartEquals("responder.available", true));
        assertThat(value, jsonPartEquals("responder.enrolled", true));
        assertThat(value, jsonPartEquals("responder.person", false));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, equalTo("1"));
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("ResponderUpdatedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
        assertThat(outgoingCloudEventMetadata.getExtension("incidentid").isPresent(), is(false));
    }

}
