package com.redhat.erdemo.responder.consumer;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.service.ResponderService;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ResponderLocationUpdatedSource {

    private final static Logger log = LoggerFactory.getLogger(ResponderLocationUpdatedSource.class);

    private static final String RESPONDER_LOCATION_UPDATED_EVENT = "ResponderLocationUpdatedEvent";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {RESPONDER_LOCATION_UPDATED_EVENT};

    @Inject
    ResponderService responderService;

    @Incoming("responder-update-location")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<CompletionStage<Void>> onMessage(IncomingKafkaRecord<String, String> message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (acceptMessage(message)) {
                    JsonObject json = new JsonObject(message.getPayload());
                    String responderId = json.getString("responderId");
                    BigDecimal lat = json.getDouble("lat") != null ? BigDecimal.valueOf(json.getDouble("lat")) : null;
                    BigDecimal lon = json.getDouble("lon") != null ? BigDecimal.valueOf(json.getDouble("lon")) : null;
                    String status = json.getString("status");
                    if (responderId != null && !"DROPPED".equalsIgnoreCase(status) && lat != null && lon != null) {
                        Responder responder = new Responder.Builder(responderId).latitude(lat).longitude(lon).build();
                        log.debug("Processing 'ResponderUpdateLocationEvent' message for responder '" + responder.getId()
                                + "' from topic:partition:offset " + message.getTopic() + ":" + message.getPartition()
                                + ":" + message.getOffset() + ". Message: " + json.toString());
                        responderService.updateResponderLocation(responder);
                    }
                }
            } catch (Exception e) {
                log.warn("Unexpected message structure: " + message.getPayload());
            }
            return message.ack();
        });
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

}
