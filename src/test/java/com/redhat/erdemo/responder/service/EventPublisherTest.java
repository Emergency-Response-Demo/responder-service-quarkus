package com.redhat.erdemo.responder.service;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.eclipse.microprofile.reactive.messaging.Message;
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

    @Test
    void testResponderCreated() {

        InMemorySink<String> results = connector.sink("responder-event");

        Responder responder = new Responder.Builder("responder123").name("John Doe").phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345")).longitude(new BigDecimal("-77.98765")).boatCapacity(10)
                .medicalKit(true).available(true).person(false).enrolled(true).build();

        eventPublisher.responderCreated(responder);

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, notNullValue());
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("messageType", "RespondersCreatedEvent"));
        assertThat(value, jsonPartEquals("invokingService", "ResponderService"));
        assertThat(value, jsonNodePresent("timestamp"));
        assertThat(value, jsonNodePresent("body"));
        assertThat(value, jsonPartEquals("body.created", 1));
        assertThat(value, jsonPartEquals("body.responders[0].id", "responder123"));
    }

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
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, notNullValue());
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("messageType", "RespondersCreatedEvent"));
        assertThat(value, jsonPartEquals("invokingService", "ResponderService"));
        assertThat(value, jsonNodePresent("timestamp"));
        assertThat(value, jsonNodePresent("body"));
        assertThat(value, jsonPartEquals("body.created", 3));
        assertThat(value, jsonPartEquals("body.responders[0].id", "responder123"));
        assertThat(value, jsonPartEquals("body.responders[1].id", "responder456"));
        assertThat(value, jsonPartEquals("body.responders[2].id", "responder789"));
    }

    @Test
    void testRespondersDeleted() {

        InMemorySink<String> results = connector.sink("responder-event");

        eventPublisher.respondersDeleted(Arrays.asList("1", "2", "3"));

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, notNullValue());
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("messageType", "RespondersDeletedEvent"));
        assertThat(value, jsonPartEquals("invokingService", "ResponderService"));
        assertThat(value, jsonNodePresent("timestamp"));
        assertThat(value, jsonNodePresent("body"));
        assertThat(value, jsonPartEquals("body.deleted", 3));
        assertThat(value, jsonPartEquals("body.responders[0]", "\"1\""));
        assertThat(value, jsonPartEquals("body.responders[1]", "\"2\""));
        assertThat(value, jsonPartEquals("body.responders[2]", "\"3\""));
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

        Map<String, String> context = new HashMap<>();
        context.put("incidentId", "qwerty");

        eventPublisher.responderUpdated(ImmutableTriple.of(true, "message", responder1), context);

        Message<String> message = results.received().get(0);
        assertThat(results.received().size(), equalTo(1));
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, equalTo("1"));
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("messageType", "ResponderUpdatedEvent"));
        assertThat(value, jsonPartEquals("invokingService", "ResponderService"));
        assertThat(value, jsonNodePresent("timestamp"));
        assertThat(value, jsonNodePresent("header"));
        assertThat(value, jsonPartEquals("header.incidentId", "qwerty"));
        assertThat(value, jsonNodePresent("body"));
        assertThat(value, jsonPartEquals("body.status", "success"));
        assertThat(value, jsonPartEquals("body.statusMessage", "message"));
        assertThat(value, jsonNodePresent("body.responder"));
        assertThat(value, jsonPartEquals("body.responder.id", "\"1\""));
        assertThat(value, jsonPartEquals("body.responder.name", "John Doe"));
        assertThat(value, jsonPartEquals("body.responder.phoneNumber", "111-222-333"));
        assertThat(value, jsonPartEquals("body.responder.latitude", 30.12345));
        assertThat(value, jsonPartEquals("body.responder.longitude", -70.98765));
        assertThat(value, jsonPartEquals("body.responder.boatCapacity", 3));
        assertThat(value, jsonPartEquals("body.responder.medicalKit", true));
        assertThat(value, jsonPartEquals("body.responder.available", true));
        assertThat(value, jsonPartEquals("body.responder.enrolled", true));
        assertThat(value, jsonPartEquals("body.responder.person", false));
    }

}
