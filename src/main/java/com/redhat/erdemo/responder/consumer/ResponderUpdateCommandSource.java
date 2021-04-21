package com.redhat.erdemo.responder.consumer;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.service.EventPublisher;
import com.redhat.erdemo.responder.service.ResponderService;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ResponderUpdateCommandSource {

    private final static Logger log = LoggerFactory.getLogger(ResponderUpdateCommandSource.class);

    private static final String UPDATE_RESPONDER_COMMAND = "UpdateResponderCommand";
    private static final String SET_RESPONDER_UNAVAILABLE_COMMAND = "SetResponderUnavailableCommand";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {UPDATE_RESPONDER_COMMAND, SET_RESPONDER_UNAVAILABLE_COMMAND};

    @Inject
    ResponderService responderService;

    @Inject
    EventPublisher eventPublisher;

    @SuppressWarnings("unchecked")
    @Incoming("responder-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<CompletionStage<Void>> onMessage(IncomingKafkaRecord<String, String> message) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                CloudEventMetadata<String> cloudEvent = acceptMessage(message);
                String incidentId = (String) cloudEvent.getExtension("incidentid").orElse(null);
                JsonObject responderJson = new JsonObject(cloudEvent.getData()).getJsonObject("responder");
                Responder responder = fromJson(responderJson);
                log.debug("Processing '" + cloudEvent.getType() + "' message for responder '" + responder.getId()
                        + "' from topic:partition:offset " + message.getTopic() + ":" + message.getPartition() + ":" + message.getOffset() +". Message: " + cloudEvent.getData());
                processMessage(responder, incidentId);
            } catch (MessageIgnoredException e) {

            } catch (Exception e) {
                log.error("Error processing msg " + message.getPayload(), e);
            }
            return message.ack();
        });
    }

    private void processMessage(Responder responder, String incidentId) {

        Triple<Boolean, String, Responder> result = responderService.updateResponder(responder);

        if (incidentId != null && !incidentId.isBlank()) {
            eventPublisher.responderSetUnavailable(result, incidentId);
        } else {
            eventPublisher.responderUpdated(result.getRight());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private CloudEventMetadata<String> acceptMessage(IncomingKafkaRecord<String, String> message) {
        try {
            Optional<IncomingCloudEventMetadata> metadata = message.getMetadata(IncomingCloudEventMetadata.class);
            if (metadata.isEmpty()) {
                log.warn("Incoming message is not a CloudEvent");
                throw new MessageIgnoredException("Incoming message is not a CloudEvent");
            }
            IncomingCloudEventMetadata<String> cloudEventMetadata = metadata.get();
            String dataContentType = cloudEventMetadata.getDataContentType().orElse("");
            if (!dataContentType.equalsIgnoreCase("application/json")) {
                log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
                throw new MessageIgnoredException("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
            }
            String type = cloudEventMetadata.getType();
            if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(type))) {
                log.debug("CloudEvent with type '" + type + "' is ignored");
                throw new MessageIgnoredException("CloudEvent with type '" + type + "' is ignored");
            }
            return metadata.get();
        } catch (Exception e) {
            log.warn("Unexpected message is ignored: " + message.getPayload());
            throw new MessageIgnoredException("Unexpected message is ignored: " + message.getPayload());
        }
    }

    private Responder fromJson(JsonObject json) {
        if (json == null || !json.containsKey("id")) {
            return null;
        }
        return new Responder.Builder(json.getString("id"))
                .name(json.getString("name"))
                .phoneNumber(json.getString("phoneNumber"))
                .medicalKit(json.getBoolean("medicalKit"))
                .boatCapacity(json.getInteger("boatCapacity"))
                .latitude(json.getDouble("latitude") != null ? BigDecimal.valueOf(json.getDouble("latitude")) : null)
                .longitude(json.getDouble("longitude") != null ? BigDecimal.valueOf(json.getDouble("longitude")) : null)
                .available(json.getBoolean("available"))
                .enrolled(json.getBoolean("enrolled"))
                .person(json.getBoolean("person"))
                .build();
    }

    private static class MessageIgnoredException extends RuntimeException {

        public MessageIgnoredException(String s) {
            super(s);
        }
    }
}
