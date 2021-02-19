package com.redhat.erdemo.responder.message;

public class RespondersDeletedEvent {

    private RespondersDeletedEvent() {}

    private int deleted;

    private String[] responders;

    public int getDeleted() {
        return deleted;
    }

    public String[] getResponders() {
        return responders;
    }

    public static class Builder {

        private final RespondersDeletedEvent event;

        public Builder(String[] responders) {
            event = new RespondersDeletedEvent();
            event.responders = responders;
            event.deleted = responders.length;
        }

        public RespondersDeletedEvent build() {
            return event;
        }
    }

}
