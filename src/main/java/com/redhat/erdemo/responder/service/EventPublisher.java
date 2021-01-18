package com.redhat.erdemo.responder.service;

import java.time.OffsetDateTime;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.erdemo.responder.message.ResponderUpdatedEvent;
import com.redhat.erdemo.responder.message.RespondersCreatedEvent;
import com.redhat.erdemo.responder.message.RespondersDeletedEvent;
import com.redhat.erdemo.responder.model.Responder;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadataBuilder;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    @Inject
    @Channel("responder-event")
    Emitter<String> emitter;

    public void responderCreated(Responder responder) {
        RespondersCreatedEvent event = new RespondersCreatedEvent.Builder(new Responder[]{responder}).build();
        emitter.send(toMessage(Integer.toString(responder.hashCode()), Json.encode(event), "RespondersCreatedEvent",null));
    }

    public void respondersCreated(List<Responder> responders) {
        RespondersCreatedEvent event = new RespondersCreatedEvent.Builder(responders.toArray(new Responder[0])).build();
        emitter.send(toMessage(Integer.toString(responders.hashCode()), Json.encode(event),"RespondersCreatedEvent",null));
    }

    public void respondersDeleted(List<String> ids) {
        RespondersDeletedEvent event = new RespondersDeletedEvent.Builder(ids.toArray(new String[0])).build();
        emitter.send(toMessage(Integer.toString(ids.hashCode()), Json.encode(event), "RespondersDeletedEvent", null));
    }

    public void responderUpdated(Triple<Boolean, String, Responder> status, String incidentId) {
        ResponderUpdatedEvent event = new ResponderUpdatedEvent.Builder(status.getLeft() ? "success" : "error", status.getRight())
                        .statusMessage(status.getMiddle()).build();
        emitter.send(toMessage(status.getRight().getId(), Json.encode(event),"ResponderUpdatedEvent", incidentId));
    }

    @SuppressWarnings("rawtypes")
    private org.eclipse.microprofile.reactive.messaging.Message<String> toMessage(String key, String payload, String messageType, String incidentId) {
        log.debug(messageType + ": " + payload);
        OutgoingCloudEventMetadataBuilder cloudEventMetadataBuilder = OutgoingCloudEventMetadata.builder().withType(messageType)
                .withTimestamp(OffsetDateTime.now().toZonedDateTime());
        if (incidentId != null) {
            cloudEventMetadataBuilder.withExtension("incidentid", incidentId);
        }
        return KafkaRecord.of(key, payload).addMetadata(cloudEventMetadataBuilder.build());
    }
}
