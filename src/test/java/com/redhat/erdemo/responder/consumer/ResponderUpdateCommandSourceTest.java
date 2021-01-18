package com.redhat.erdemo.responder.consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.math.BigDecimal;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.service.EventPublisher;
import com.redhat.erdemo.responder.service.ResponderService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
public class ResponderUpdateCommandSourceTest {

    @Inject
    ResponderUpdateCommandSource source;

    @InjectMock
    ResponderService responderService;

    @InjectMock
    EventPublisher eventPublisher;

    @Captor
    ArgumentCaptor<Responder> responderCaptor;

    boolean messageAck = false;

    @BeforeEach
    void init() {
        openMocks(this);
        messageAck = false;
    }

    @Test
    void testProcessMessage() throws ExecutionException, InterruptedException {
        String json = "{" +
                "\"responder\" : {" +
                "\"id\" : \"1\"," +
                "\"available\" : false" +
                "} " +
                "}";

        Responder updated = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .longitude(new BigDecimal("30.12345"))
                .latitude(new BigDecimal("-77.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .build();

        when(responderService.updateResponder(any(Responder.class))).thenReturn(new ImmutableTriple<>(true, "ok", updated));

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json, true, "application/json", "UpdateResponderCommand", "incident"));
        c.toCompletableFuture().get();

        verify(responderService).updateResponder(responderCaptor.capture());
        Responder captured = responderCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getId(), equalTo("1"));
        assertThat(captured.isAvailable(), equalTo(false));
        assertThat(captured.getName(), nullValue());
        assertThat(captured.getPhoneNumber(), nullValue());
        assertThat(captured.getLatitude(), nullValue());
        assertThat(captured.getLongitude(), nullValue());
        assertThat(captured.getBoatCapacity(), nullValue());
        assertThat(captured.isMedicalKit(), nullValue());

        verify(eventPublisher).responderUpdated(eq(new ImmutableTriple<>(true, "ok", updated)), eq("incident"));

        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageUpdateResponderNoIncidentIdHeader() throws ExecutionException, InterruptedException {

        String json = "{" +
                "\"responder\" : {" +
                "\"id\" : \"2\"," +
                "\"available\" : false" +
                "} " +
                "}";

        Responder updated = new Responder.Builder("2")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .longitude(new BigDecimal("30.12345"))
                .latitude(new BigDecimal("-77.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .build();
        when(responderService.updateResponder(any(Responder.class))).thenReturn(new ImmutableTriple<>(true, "ok", updated));

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("2", json, true, "application/json", "UpdateResponderCommand", null));
        c.toCompletableFuture().get();

        verify(responderService).updateResponder(responderCaptor.capture());
        Responder captured = responderCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getId(), equalTo("2"));
        assertThat(captured.isAvailable(), equalTo(false));
        assertThat(captured.getName(), nullValue());
        assertThat(captured.getPhoneNumber(), nullValue());
        assertThat(captured.getLatitude(), nullValue());
        assertThat(captured.getLongitude(), nullValue());
        assertThat(captured.getBoatCapacity(), nullValue());
        assertThat(captured.isMedicalKit(), nullValue());

        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(String.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageWrongMessageType() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("3", json, true, "application/json", "WrongMessageType", "incident"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(String.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageWrongDataContentType() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("4", json, true, "application/avro", "UpdateResponderCommand", "incident"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(String.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageNoDataContentType() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("5", json, true, null, "UpdateResponderCommand", "incident"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(String.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageNotACloudEvent() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("6", json, false, "application/json", "UpdateResponderCommand", "incident"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(String.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageWrongMessage() throws ExecutionException, InterruptedException {
        String json = "{\"field1\":\"value1\"," +
                "\"field2\":\"value2\"}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("7", json, true, "application/json", "UpdateResponderCommand", "incident"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(String.class));
        assertThat(messageAck, equalTo(true));
    }

    private IncomingKafkaRecord<String, String> toRecord(String key, String payload, boolean cloudEvent, String dataContentType, String messageType, String incidentId) {

        MockKafkaConsumer<String, String> mc = new MockKafkaConsumer<>();
        ConsumerRecord<String, String> cr;
        if (cloudEvent) {
            RecordHeaders headers = new RecordHeaders();
            headers.add("ce_specversion", "1.0".getBytes());
            headers.add("ce_id", "18cb49fe-9353-4856-9a0c-d66fe1237c86".getBytes());
            headers.add("ce_type", messageType.getBytes());
            headers.add("ce_source", "test".getBytes());
            headers.add("ce_time", "2020-12-30T19:54:20.765566GMT".getBytes());
            if (dataContentType != null) {
                headers.add("ce_datacontenttype", dataContentType.getBytes());
                headers.add("content-type", dataContentType.getBytes());
            }
            if (incidentId != null) {
                headers.add("ce_incidentid", incidentId.getBytes());
            }

            cr = new ConsumerRecord<>("topic", 1, 100, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                    (long) ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, key, payload, headers);
        } else {
            cr = new ConsumerRecord<>("topic", 1, 100, key, payload);
        }
        KafkaConsumerRecord<String, String> kcr = new KafkaConsumerRecord<>(new KafkaConsumerRecordImpl<>(cr));
        KafkaCommitHandler kch = new KafkaCommitHandler() {
            @Override
            public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
                Uni<Void> uni = AsyncResultUni.toUni(mc::commit);
                return uni.subscribeAsCompletionStage();
            }
        };
        return new IncomingKafkaRecord<>(kcr, kch, null, true, false);
    }

    private class MockKafkaConsumer<K, V> extends KafkaConsumerImpl<K, V> {

        public MockKafkaConsumer() {
            super(new KafkaReadStreamImpl<>(null, null));
        }

        @Override
        public void commit(Handler<AsyncResult<Void>> completionHandler) {
            ResponderUpdateCommandSourceTest.this.messageAck = true;
            Promise<Void> future = Promise.promise();
            future.future().onComplete(completionHandler);
            future.complete(null);
        }
    }
}
