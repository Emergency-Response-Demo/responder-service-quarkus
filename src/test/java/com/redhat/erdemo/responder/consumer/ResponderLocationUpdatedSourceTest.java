package com.redhat.erdemo.responder.consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
public class ResponderLocationUpdatedSourceTest {

    @InjectMock
    ResponderService responderService;

    @Inject
    ResponderLocationUpdatedSource source;

    @Captor
    ArgumentCaptor<Responder> responderCaptor;

    boolean messageAck = false;

    @BeforeEach
    void init() {
        openMocks(this);
        messageAck = false;
    }

    @Test
    void testResponderLocationUpdated() throws ExecutionException, InterruptedException {
        String json = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";


        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json, true, "application/json","ResponderLocationUpdatedEvent" ));
        c.toCompletableFuture().get();

        verify(responderService).updateResponderLocation(responderCaptor.capture());
        Responder captured = responderCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getId(), equalTo("64"));
        assertThat(captured.getLatitude().toString(), equalTo("34.1701"));
        assertThat(captured.getLongitude().toString(), equalTo("-77.9482"));
        assertThat(captured.getName(), nullValue());
        assertThat(captured.getPhoneNumber(), nullValue());
        assertThat(captured.isMedicalKit(), nullValue());
        assertThat(captured.getBoatCapacity(), nullValue());
        assertThat(captured.isAvailable(), nullValue());
        assertThat(captured.isEnrolled(), nullValue());
        assertThat(captured.isPerson(), nullValue());

        assertThat(messageAck, equalTo(true));

    }

    @Test
    public void testResponderLocationUpdateEventStatusNotMoving() throws ExecutionException, InterruptedException {
        String json = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"DROPPED\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json, true, "application/json","ResponderLocationUpdatedEvent" ));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testResponderLocationUpdateEventMissingResponderId() throws ExecutionException, InterruptedException {
        String json = "{\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json, true, "application/json","ResponderLocationUpdatedEvent" ));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testResponderLocationUpdateEventMissingLatLon() throws ExecutionException, InterruptedException {
        String json = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"DROPPED\",\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json, true, "application/json","ResponderLocationUpdatedEvent" ));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testProcessMessageNotACloudEvent() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json, false, "application/json", "ResponderLocationUpdatedEvent"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testProcessMessageWrongMessageType() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json, true, "application/json", "WrongMessageType"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testProcessMessageWrongDataContentType() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json, true, "application/avro", "ResponderLocationUpdatedEvent"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testProcessMessageNoDataContentType() throws ExecutionException, InterruptedException {

        String json = "{}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("1", json, true, null, "ResponderLocationUpdatedEvent"));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    private IncomingKafkaRecord<String, String> toRecord(String key, String payload, boolean cloudEvent, String dataContentType, String messageType) {

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
            ResponderLocationUpdatedSourceTest.this.messageAck = true;
            Promise<Void> future = Promise.promise();
            future.future().onComplete(completionHandler);
            future.complete(null);
        }
    }

}
