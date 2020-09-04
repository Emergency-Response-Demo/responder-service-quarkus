package com.redhat.erdemo.responder.service;

import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;

import com.redhat.erdemo.responder.message.Message;
import com.redhat.erdemo.responder.message.ResponderUpdatedEvent;
import com.redhat.erdemo.responder.message.RespondersCreatedEvent;
import com.redhat.erdemo.responder.message.RespondersDeletedEvent;
import com.redhat.erdemo.responder.model.Responder;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final UnicastProcessor<Pair<String, Message<?>>> processor = UnicastProcessor.create();

    public void responderCreated(Responder responder) {
        Message<RespondersCreatedEvent> message = new Message.Builder<>("RespondersCreatedEvent", "ResponderService",
                new RespondersCreatedEvent.Builder(new Responder[]{responder}).build()).build();
        processor.onNext(ImmutablePair.of(Integer.toString(responder.hashCode()), message));
    }

    public void respondersCreated(List<Responder> responders) {
        Message<RespondersCreatedEvent> message = new Message.Builder<>("RespondersCreatedEvent", "ResponderService",
                new RespondersCreatedEvent.Builder(responders.toArray(new Responder[0])).build()).build();
        processor.onNext(ImmutablePair.of(Integer.toString(responders.hashCode()), message));
    }

    public void respondersDeleted(List<String> ids) {
        Message<RespondersDeletedEvent> message = new Message.Builder<>("RespondersDeletedEvent", "ResponderService",
                new RespondersDeletedEvent.Builder(ids.toArray(new String[0])).build()).build();
        processor.onNext(ImmutablePair.of(Integer.toString(ids.hashCode()), message));
    }

    public void responderUpdated(Triple<Boolean, String, Responder> status, Map<String, String> context) {
        Message.Builder<ResponderUpdatedEvent> builder = new Message.Builder<>("ResponderUpdatedEvent", "ResponderService",
                new ResponderUpdatedEvent.Builder(status.getLeft() ? "success" : "error", status.getRight())
                        .statusMessage(status.getMiddle()).build());
        context.forEach(builder::header);
        processor.onNext(ImmutablePair.of(status.getRight().getId(), builder.build()));
    }

    @Outgoing("responder-event")
    public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> responderEvent() {
        return processor.onItem().apply(this::toMessage);
    }

    private org.eclipse.microprofile.reactive.messaging.Message<String> toMessage(Pair<String, Message<?>> pair) {

        return KafkaRecord.of(pair.getLeft(), Json.encode(pair.getRight()));
    }



}
