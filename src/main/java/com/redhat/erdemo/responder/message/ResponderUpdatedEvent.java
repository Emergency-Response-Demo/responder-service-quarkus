package com.redhat.erdemo.responder.message;

import com.redhat.erdemo.responder.model.Responder;

public class ResponderUpdatedEvent {

    private Responder responder;

    public Responder getResponder() {
        return responder;
    }

    public static class Builder {

        private final ResponderUpdatedEvent event;

        public Builder(Responder responder) {
            event = new ResponderUpdatedEvent();
            event.responder = responder;
        }

        public ResponderUpdatedEvent build() {
            return event;
        }
    }

}
