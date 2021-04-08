package com.redhat.erdemo.responder.rest;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.model.ResponderStats;
import com.redhat.erdemo.responder.service.ResponderService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
public class ResponderResourceTest {

    @InjectMock
    ResponderService responderService;

    @Captor
    private ArgumentCaptor<Responder> responderCaptor;

    @Captor
    private ArgumentCaptor<List<Responder>> responderListCaptor;

    @Test
    void testStatsEndpoint() throws JsonProcessingException {
        when(responderService.getResponderStats()).thenReturn(new ResponderStats(5L, 10L));

        String body = given().when().get("/stats")
            .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json = new ObjectMapper().writeValueAsString(new ResponderStats(5L, 10L));
        assertThat(body, jsonEquals(json));
        verify(responderService).getResponderStats();
    }

    @Test
    void testResponderByIdEndpoint() throws JsonProcessingException {

        Responder responder = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderService.getResponder(any(Long.class))).thenReturn(responder);

        String body = given().when().get("/responder/1")
            .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responder);
        assertThat(body, jsonEquals(json));
        verify(responderService).getResponder(1L);
    }

    @Test
    void testResponderByIdEndpointWhenNotFound() {

        when(responderService.getResponder(any(Long.class))).thenReturn(null);

        given().when().get("/responder/1")
                .then().assertThat().statusCode(404).body(equalTo(""));
        verify(responderService).getResponder(1L);
    }

    @Test
    void testResponderByNameEndpoint() throws JsonProcessingException {

        Responder responder = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderService.getResponderByName(any(String.class))).thenReturn(responder);

        String body = given().urlEncodingEnabled(false).when().get("/responder/byname/John%20Doe")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responder);
        assertThat(body, jsonEquals(json));
        verify(responderService).getResponderByName("John Doe");
    }

    @Test
    void testResponderByNameEndpointWhenNotFound() {

        when(responderService.getResponder(any(Long.class))).thenReturn(null);

        given().urlEncodingEnabled(false).when().get("/responder/byname/John%20Doe")
                .then().assertThat().statusCode(404).body(equalTo(""));
        verify(responderService).getResponderByName("John Doe");
    }

    @Test
    void testAvailableRespondersEndpoint() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.availableResponders()).thenReturn(responders);

        String body = given().when().get("/responders/available")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).availableResponders();
    }

    @Test
    void testAvailableRespondersEndpointWithLimit() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.availableResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders/available?limit=10")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).availableResponders(10, 0);
    }

    @Test
    void testAvailableRespondersEndpointWithLimitAndOffset() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.availableResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders/available?limit=10&offset=5")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).availableResponders(10, 5);
    }

    @Test
    void testAvailableRespondersEndpointWhenServiceReturnsEmptyList() throws JsonProcessingException {

        List<Responder> responders = Collections.emptyList();

        when(responderService.availableResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders/available?limit=10&offset=5")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).availableResponders(10, 5);
    }

    @Test
    void testAllRespondersEndpoint() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.allResponders()).thenReturn(responders);

        String body = given().when().get("/responders")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).allResponders();
    }

    @Test
    void testAllRespondersEndpointWithLimit() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.allResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders?limit=10")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).allResponders(10, 0);
    }

    @Test
    void testAllRespondersEndpointWithLimitAndOffset() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.allResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders?limit=10&offset=5")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).allResponders(10, 5);
    }

    @Test
    void testAllRespondersEndpointWhenServiceReturnsEmptyList() throws JsonProcessingException {

        List<Responder> responders = Collections.emptyList();

        when(responderService.allResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders?limit=10&offset=5")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).allResponders(10, 5);
    }

    @Test
    void testPersonRespondersEndpoint() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.personResponders()).thenReturn(responders);

        String body = given().when().get("/responders/person")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).personResponders();
    }

    @Test
    void testPersonRespondersEndpointWithLimit() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.personResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders/person?limit=10")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).personResponders(10, 0);
    }

    @Test
    void testPersonRespondersEndpointWithLimitAndOffset() throws JsonProcessingException {

        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        Responder responder2 = new Responder.Builder("1")
                .name("John Foo")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<Responder> responders = Arrays.asList(responder1, responder2);

        when(responderService.personResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders/person?limit=10&offset=5")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).personResponders(10, 5);
    }

    @Test
    void testPersonRespondersEndpointWhenServiceReturnsEmptyList() throws JsonProcessingException {

        List<Responder> responders = Collections.emptyList();

        when(responderService.personResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);

        String body = given().when().get("/responders/person?limit=10&offset=5")
                .then().assertThat().statusCode(200).contentType(ContentType.JSON).extract().asString();

        String json =  new ObjectMapper().writeValueAsString(responders);
        assertThat(body, jsonEquals(json));
        verify(responderService).personResponders(10, 5);
    }

    @Test
    void testCreateResponderEndpoint() {

        openMocks(this);

        when(responderService.createResponder(any(Responder.class))).thenReturn(
                new Responder.Builder("1").name("John Foo").phoneNumber("111-222-333").latitude(new BigDecimal("30.12345"))
                        .longitude(new BigDecimal("-70.98765")).boatCapacity(3).medicalKit(true).available(true)
                        .enrolled(false).person(true).build());

        String body = "{\"available\":true,\"boatCapacity\":3,\"enrolled\":false,\"latitude\":30.12345,"
                + "\"longitude\":-70.98765,\"medicalKit\":true,\"name\":\"John Foo\",\"person\":true,"
                + "\"phoneNumber\":\"111-222-333\"}";

        given().when().with().body(body).header(new Header("Content-Type", "application/json"))
            .post("/responder")
            .then().assertThat().statusCode(201).header("Location", endsWith("/responder/1")).body(equalTo(""));

        verify(responderService).createResponder(responderCaptor.capture());
        Responder responder = responderCaptor.getValue();
        assertThat(responder, notNullValue());
        assertThat(responder.getId(), nullValue());
        assertThat(responder.getBoatCapacity(), equalTo(3));
        assertThat(responder.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(responder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(responder.getName(), equalTo("John Foo"));
        assertThat(responder.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(responder.isMedicalKit(), equalTo(true));
        assertThat(responder.isAvailable(), equalTo(true));
        assertThat(responder.isEnrolled(), equalTo(false));
        assertThat(responder.isPerson(), equalTo(true));
    }

    @Test
    void testCreateRespondersEndpoint() {

        openMocks(this);

        String body = "[{\"available\":true,\"boatCapacity\":3,\"enrolled\":false,\"latitude\":30.12345,"
                + "\"longitude\":-70.98765,\"medicalKit\":true,\"name\":\"John Doe\",\"person\":true,"
                + "\"phoneNumber\":\"111-222-333\"},"
                + "{\"available\":false,\"boatCapacity\":5,\"enrolled\":true,\"latitude\":30.98765,"
                + "\"longitude\":-70.12345,\"medicalKit\":true,\"name\":\"Jane Foo\",\"person\":false,"
                + "\"phoneNumber\":\"111-222-333\"}]";

        given().when().with().body(body).header(new Header("Content-Type", "application/json"))
                .post("/responders")
                .then().assertThat().statusCode(201).body(equalTo(""));

        verify(responderService).createResponders(responderListCaptor.capture());
        List<Responder> responders = responderListCaptor.getValue();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getName(), CoreMatchers.anyOf(equalTo("John Doe"), equalTo("Jane Foo")));
        if (responder.getName().equals("Jane Foo")) {
            responder = responders.get(1);
        }
        assertThat(responder.getId(), nullValue());
        assertThat(responder.getBoatCapacity(), equalTo(3));
        assertThat(responder.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(responder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(responder.getName(), equalTo("John Doe"));
        assertThat(responder.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(responder.isMedicalKit(), equalTo(true));
        assertThat(responder.isAvailable(), equalTo(true));
        assertThat(responder.isEnrolled(), equalTo(false));
        assertThat(responder.isPerson(), equalTo(true));
    }

    @Test
    void testUpdateResponderEndpoint() {

        openMocks(this);

        String body = "{\"id\":\"1\",\"available\":true,\"latitude\":30.12345,\"longitude\":-70.98765}";

        given().when().with().body(body).header(new Header("Content-Type", "application/json"))
                .put("/responder")
                .then().assertThat().statusCode(204).body(equalTo(""));

        verify(responderService).updateResponder(responderCaptor.capture());
        Responder responder = responderCaptor.getValue();
        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("1"));
        assertThat(responder.getBoatCapacity(), nullValue());
        assertThat(responder.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(responder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(responder.getName(), nullValue());
        assertThat(responder.getPhoneNumber(), nullValue());
        assertThat(responder.isMedicalKit(), nullValue());
        assertThat(responder.isAvailable(), equalTo(true));
        assertThat(responder.isEnrolled(), nullValue());
        assertThat(responder.isPerson(), nullValue());
    }

    @Test
    void testResetEndpoint() {

        given().when().post("/responders/reset")
                .then().assertThat().statusCode(200).body(equalTo(""));

        verify(responderService).reset();
    }

    @Test
    void testClearEndpoint() {

        given().when().post("/responders/clear")
                .then().assertThat().statusCode(200).body(equalTo(""));

        verify(responderService).clear(false);
    }

    @Test
    void testClearEndpointDeleteBots() {

        given().when().post("/responders/clear?delete=bots")
                .then().assertThat().statusCode(200).body(equalTo(""));

        verify(responderService).clear(true);
    }

    @Test
    void testClearEndpointDelete() {

        given().when().post("/responders/clear?delete=randomValue")
                .then().assertThat().statusCode(200).body(equalTo(""));

        verify(responderService).clear(false);
    }

    @Test
    void testClearEndpointDeleteAll() {

        given().when().post("/responders/clear?delete=all")
                .then().assertThat().statusCode(200).body(equalTo(""));

        verify(responderService).deleteAll();
    }
}
