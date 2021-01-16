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


        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json));
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

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        assertThat(messageAck, equalTo(true));
    }

    @Test
    public void testResponderLocationUpdateEventWrongMessage() throws ExecutionException, InterruptedException {
        String json = "{\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        CompletionStage<CompletionStage<Void>> c =  source.onMessage(toRecord("64", json));
        c.toCompletableFuture().get();

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
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
            ResponderLocationUpdatedSourceTest.this.messageAck = true;
            Promise<Void> future = Promise.promise();
            future.future().onComplete(completionHandler);
            future.complete(null);
        }
    }

}
