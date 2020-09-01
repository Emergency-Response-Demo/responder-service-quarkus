package com.redhat.erdemo.responder.consumer;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.service.ResponderService;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ResponderLocationUpdatedSource {

    private final static Logger log = LoggerFactory.getLogger(ResponderLocationUpdatedSource.class);

    @Inject
    ResponderService responderService;

    @Incoming("responder-update-location")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<CompletionStage<Void>> onMessage(IncomingKafkaRecord<String, String> message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                JsonObject json = new JsonObject(message.getPayload());
                String responderId = json.getString("responderId");
                BigDecimal lat = json.getDouble("lat") != null ? BigDecimal.valueOf(json.getDouble("lat")) : null;
                BigDecimal lon = json.getDouble("lon") != null ? BigDecimal.valueOf(json.getDouble("lon")) : null;
                String status = json.getString("status");
                if (responderId != null && !"DROPPED".equalsIgnoreCase(status)) {
                    Responder responder = new Responder.Builder(responderId).latitude(lat).longitude(lon).build();
                    log.debug("Processing 'ResponderUpdateLocationEvent' message for responder '" + responder.getId()
                            + "' from topic:partition:offset " + message.getTopic() + ":" + message.getPartition()
                            + ":" + message.getOffset() + ". Message: " + json.toString());
                    responderService.updateResponderLocation(responder);
                }

            } catch (Exception e) {
                log.warn("Unexpected message structure: " + message.getPayload());
            }
            return message.ack();
        });
    }

}
