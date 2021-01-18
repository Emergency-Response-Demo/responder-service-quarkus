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
    private static final String[] ACCEPTED_MESSAGE_TYPES = {UPDATE_RESPONDER_COMMAND};

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
                if (acceptMessage(message)) {
                    String incidentId = (String) message.getMetadata(IncomingCloudEventMetadata.class).get().getExtension("incidentid").orElse(null);
                    processMessage(new JsonObject(message.getPayload()), incidentId, message.getTopic(), message.getPartition(), message.getOffset());
                }
            } catch (Exception e) {
                log.error("Error processing msg " + message.getPayload(), e);
            }
            return message.ack();
        });
    }

    private void processMessage(JsonObject json, String incidentId, String topic, int partition, long offset) {
        JsonObject responderJson = json.getJsonObject("responder");
        Responder responder = fromJson(responderJson);

        log.debug("Processing '" + UPDATE_RESPONDER_COMMAND + "' message for responder '" + responder.getId()
                + "' from topic:partition:offset " + topic + ":" + partition + ":" + offset +". Message: " + json.toString());

        Triple<Boolean, String, Responder> result = responderService.updateResponder(responder);

        if (incidentId != null) {
            eventPublisher.responderUpdated(result, incidentId);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean acceptMessage(IncomingKafkaRecord<String, String> message) {
        try {
            Optional<IncomingCloudEventMetadata> metadata = message.getMetadata(IncomingCloudEventMetadata.class);
            if (metadata.isEmpty()) {
                log.warn("Incoming message is not a CloudEvent");
                return false;
            }
            IncomingCloudEventMetadata<String> cloudEventMetadata = metadata.get();
            String dataContentType = cloudEventMetadata.getDataContentType().orElse("");
            if (!dataContentType.equalsIgnoreCase("application/json")) {
                log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
                return false;
            }
            String type = cloudEventMetadata.getType();
            if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(type))) {
                log.debug("CloudEvent with type '" + type + "' is ignored");
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("Unexpected message is ignored: " + message.getPayload());
            return false;
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
                .enrolled(json.getBoolean("enrolled)"))
                .person(json.getBoolean("person"))
                .build();
    }
}
