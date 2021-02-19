package com.redhat.erdemo.responder.message;

import com.redhat.erdemo.responder.model.Responder;

public class RespondersCreatedEvent {

    private RespondersCreatedEvent() {}

    private int created;

    private Responder[] responders;

    public int getCreated() {
        return created;
    }

    public Responder[] getResponders() {
        return responders;
    }

    public static class Builder {

        private final RespondersCreatedEvent event;

        public Builder(Responder[] responders) {
            event = new RespondersCreatedEvent();
            event.responders = responders;
            event.created = responders.length;
        }

        public RespondersCreatedEvent build() {
            return event;
        }
    }

}
