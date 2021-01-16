package com.redhat.erdemo.responder.consumer;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.service.EventPublisher;
import com.redhat.erdemo.responder.service.ResponderService;
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

    @Incoming("responder-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<CompletionStage<Void>> onMessage(IncomingKafkaRecord<String, String> message) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                acceptMessage(message.getPayload()).ifPresent(j -> processMessage(j, message.getTopic(), message.getPartition(), message.getOffset()));
            } catch (Exception e) {
                log.error("Error processing msg " + message.getPayload(), e);
            }
            return message.ack();
        });
    }

    private void processMessage(JsonObject json, String topic, int partition, long offset) {
        JsonObject responderJson = json.getJsonObject("body").getJsonObject("responder");
        Responder responder = fromJson(responderJson);

        log.debug("Processing '" + UPDATE_RESPONDER_COMMAND + "' message for responder '" + responder.getId()
                + "' from topic:partition:offset " + topic + ":" + partition + ":" + offset +". Message: " + json.toString());

        Triple<Boolean, String, Responder> result = responderService.updateResponder(responder);

        Map<String, String> headers = json.getJsonObject("header", new JsonObject()).getMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        if (headers.containsKey("incidentId")) {
            eventPublisher.responderUpdated(result, headers);
        }
    }

    private Optional<JsonObject> acceptMessage(String messageAsJson) {
        try {
            JsonObject json = new JsonObject(messageAsJson);
            String messageType = json.getString("messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType)) {
                if (json.containsKey("body") && json.getJsonObject("body").containsKey("responder")) {
                    return Optional.of(json);
                }
            }
            log.debug("Message with type '" + messageType + "' is ignored");
        } catch (Exception e) {
            log.warn("Unexpected message which is not JSON or without 'messageType' field.");
            log.warn("Message: " + messageAsJson);
        }
        return Optional.empty();
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
