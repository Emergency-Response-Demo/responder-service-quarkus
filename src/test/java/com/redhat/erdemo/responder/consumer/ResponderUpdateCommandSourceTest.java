package com.redhat.erdemo.responder.consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.math.BigDecimal;
import java.util.Map;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;

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

    @Captor
    ArgumentCaptor<Map<String, String>> headerCaptor;

    boolean messageAck = false;

    @BeforeEach
    void init() {
        openMocks(this);
        messageAck = false;
    }

    @Test
    void testProcessMessage() throws ExecutionException, InterruptedException {
        String json = "{\"messageType\" : \"UpdateResponderCommand\"," +
                "\"id\" : \"messageId\"," +
                "\"invokingService\" : \"messageSender\"," +
                "\"timestamp\" : 1521148332397," +
                "\"header\" : {" +
                "\"incidentId\" : \"incident\"" +
                "}," +
                "\"body\" : {" +
                "\"responder\" : {" +
                "\"id\" : \"1\"," +
                "\"available\" : false" +
                "} " +
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

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json));
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

        verify(eventPublisher).responderUpdated(Mockito.eq(new ImmutableTriple<>(true, "ok", updated)), headerCaptor.capture());
        Map<String, String> headers = headerCaptor.getValue();
        assertThat(headers, notNullValue());
        assertThat(headers.containsKey("incidentId"), equalTo(true));
        assertThat(headers.get("incidentId"), equalTo("incident"));

        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageUpdateResponderNoIncidentIdHeader() throws ExecutionException, InterruptedException {

        String json = "{\"messageType\" : \"UpdateResponderCommand\"," +
                "\"id\" : \"messageId\"," +
                "\"invokingService\" : \"messageSender\"," +
                "\"timestamp\" : 1521148332397," +
                "\"body\" : {" +
                "\"responder\" : {" +
                "\"id\" : \"1\"," +
                "\"available\" : false" +
                "} " +
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

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json));
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

        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(Map.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageWrongMessageType() throws ExecutionException, InterruptedException {

        String json = "{\"messageType\":\"WrongType\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\":{} " +
                "}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(Map.class));
        assertThat(messageAck, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessMessageWrongMessage() throws ExecutionException, InterruptedException {
        String json = "{\"field1\":\"value1\"," +
                "\"field2\":\"value2\"}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(eventPublisher, never()).responderUpdated(any(Triple.class), any(Map.class));
        assertThat(messageAck, equalTo(true));
    }

    private IncomingKafkaRecord<String, String> toRecord(String key, String payload) {

        MockKafkaConsumer<String, String> mc = new MockKafkaConsumer<>();
        ConsumerRecord<String, String> cr = new ConsumerRecord<>("topic", 1, 100, key, payload);
        KafkaConsumerRecord<String, String> kcr = new KafkaConsumerRecord<>(new KafkaConsumerRecordImpl<>(cr));
        KafkaCommitHandler kch = new KafkaCommitHandler() {
            @Override
            public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
                Uni<Void> uni = AsyncResultUni.toUni(mc::commit);
                return uni.subscribeAsCompletionStage();
            }
        };
        return new IncomingKafkaRecord<>(kcr, kch, null, false, false);
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
