package com.redhat.erdemo.responder.message;

import com.redhat.erdemo.responder.model.Responder;

public class ResponderSetUnavailableEvent {

    private String status;

    private String statusMessage;

    private Responder responder;

    public String getStatus() {
        return status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public Responder getResponder() {
        return responder;
    }

    public static class Builder {

        private final ResponderSetUnavailableEvent event;

        public Builder(String status, Responder responder) {
            event = new ResponderSetUnavailableEvent();
            event.responder = responder;
            event.status = status;
        }

        public Builder statusMessage(String statusMessage) {
            event.statusMessage = statusMessage;
            return this;
        }

        public ResponderSetUnavailableEvent build() {
            return event;
        }
    }

}
