package com.redhat.erdemo.responder.service;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.model.ResponderStats;
import com.redhat.erdemo.responder.repository.ResponderEntity;
import com.redhat.erdemo.responder.repository.ResponderRepository;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.stubbing.Answer;

@QuarkusTest
public class ResponderServiceTest {

    @Inject
    ResponderService responderService;

    @InjectMock
    ResponderRepository responderRepository;

    @InjectMock
    EventPublisher eventPublisher;

    @Captor
    private ArgumentCaptor<ResponderEntity> entityCaptor;

    @Captor
    private ArgumentCaptor<List<Responder>> responderListCaptor;

    @BeforeEach
    void init() {
        initMocks(this);
    }

    @Test
    void testGetResponderStats() {

        when(responderRepository.enrolledRespondersCount()).thenReturn(10L);
        when(responderRepository.activeRespondersCount()).thenReturn(5L);

        ResponderStats stats = responderService.getResponderStats();

        assertThat(stats, CoreMatchers.notNullValue());
        assertThat(stats.getTotal(), equalTo(10L));
        assertThat(stats.getActive(), equalTo(5L));
        verify(responderRepository).enrolledRespondersCount();
        verify(responderRepository).activeRespondersCount();
    }

    @Test
    public void testFindResponderById() {
        ResponderEntity found = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderRepository.findById(any(Long.class))).thenReturn(found);

        Responder responder = responderService.getResponder(1);

        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("1"));
        assertThat(responder.getName(), equalTo("John Doe"));

        verify(responderRepository).findById(eq(1L));
    }

    @Test
    public void testFindResponderByIdWhenNotFound() {

        when(responderRepository.findById(any(Long.class))).thenReturn(null);

        Responder responder = responderService.getResponder(1);

        assertThat(responder, nullValue());

        verify(responderRepository).findById(eq(1L));
    }

    @Test
    public void testFindByName() {

        ResponderEntity found = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderRepository.findByName(any(String.class))).thenReturn(found);

        Responder responder = responderService.getResponderByName("John Doe");

        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("1"));
        assertThat(responder.getName(), equalTo("John Doe"));
        assertThat(responder.isAvailable(), equalTo(true));
        assertThat(responder.isPerson(), equalTo(true));
        assertThat(responder.isEnrolled(), equalTo(true));

        verify(responderRepository).findByName(eq("John Doe"));
    }

    @Test
    public void testFindByNameWhenNotFound() {

        when(responderRepository.findByName(any(String.class))).thenReturn(null);

        Responder responder = responderService.getResponderByName("John Doe");

        assertThat(responder, nullValue());

        verify(responderRepository).findByName(eq("John Doe"));
    }

    @Test
    public void testAvailableResponders() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(responder1, responder2);

        when(responderRepository.availableResponders()).thenReturn(responderEntities);

        List<Responder> responders = responderService.availableResponders();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
        verify(responderRepository).availableResponders();
    }

    @Test
    public void testAvailableRespondersWithLimitAndOffset() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(responder1, responder2);

        when(responderRepository.availableResponders(any(Integer.class), any(Integer.class))).thenReturn(responderEntities);

        List<Responder> responders = responderService.availableResponders(10,0);
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
        verify(responderRepository).availableResponders(10, 0);
    }

    @Test
    public void testAllResponders() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(responder1, responder2);

        when(responderRepository.allResponders()).thenReturn(responderEntities);

        List<Responder> responders = responderService.allResponders();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
        verify(responderRepository).allResponders();
    }

    @Test
    public void testAllRespondersWithLimitAndOffset() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(responder1, responder2);

        when(responderRepository.allResponders(any(Integer.class), any(Integer.class))).thenReturn(responderEntities);

        List<Responder> responders = responderService.allResponders(10, 0);
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
        verify(responderRepository).allResponders(10, 0);
    }

    @Test
    public void testPersonResponders() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(responder1, responder2);

        when(responderRepository.personResponders()).thenReturn(responderEntities);

        List<Responder> responders = responderService.personResponders();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
        verify(responderRepository).personResponders();
    }

    @Test
    public void testPersonRespondersWithLimitAndOffset() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(responder1, responder2);

        when(responderRepository.personResponders(any(Integer.class), any(Integer.class))).thenReturn(responderEntities);

        List<Responder> responders = responderService.personResponders(10, 0);
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
        verify(responderRepository).personResponders(10, 0);
    }

    @Test
    public void testCreateResponder() {

        Responder toCreate = new Responder.Builder(null)
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

        when(responderRepository.create(any(ResponderEntity.class))).thenAnswer((Answer<ResponderEntity>) invocation -> {
            ResponderEntity entity = invocation.getArgument(0);
            setField(entity, "id", 100L);
            return entity;
        });

        Responder created = responderService.createResponder(toCreate);

        verify(responderRepository).create(entityCaptor.capture());
        ResponderEntity entity = entityCaptor.getValue();
        assertThat(entity.getName(), equalTo("John Doe"));

        assertThat(created, notNullValue());
        assertThat(created.getId(), equalTo("100"));
        assertThat(created.getName(), equalTo("John Doe"));
        assertThat(created.isAvailable(), equalTo(true));
        assertThat(created.isPerson(), equalTo(true));
        assertThat(created.isEnrolled(), equalTo(true));

        verify(eventPublisher).responderCreated(created);
    }

    @Test
    public void testCreateResponders() {

        Responder toCreate1 = new Responder.Builder(null)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        Responder toCreate2 = new Responder.Builder(null)
                .name("John Foo")
                .phoneNumber("222-333-4443")
                .latitude(new BigDecimal("31.12345"))
                .longitude(new BigDecimal("-71.98765"))
                .boatCapacity(6)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        when(responderRepository.create(any(ResponderEntity.class))).thenAnswer((Answer<ResponderEntity>) invocation -> {
            ResponderEntity entity = invocation.getArgument(0);
            setField(entity, "id", 1);
            return entity;
        }).thenAnswer((Answer<ResponderEntity>) invocation -> {
            ResponderEntity entity = invocation.getArgument(0);
            setField(entity, "id", 2);
            return entity;
        });

        responderService.createResponders(Arrays.asList(toCreate1, toCreate2));

        verify(responderRepository, times(2)).create(entityCaptor.capture());
        List<ResponderEntity> responderEntities = entityCaptor.getAllValues();
        assertThat(responderEntities.size(), equalTo(2));
        assertThat(responderEntities.get(0).getName(), equalTo("John Doe"));
        assertThat(responderEntities.get(1).getName(), equalTo("John Foo"));

        verify(eventPublisher).respondersCreated(responderListCaptor.capture());
        List<Responder> responderList = responderListCaptor.getValue();
        assertThat(responderList, notNullValue());
        assertThat(responderList.size(), equalTo(2));
    }

    @Test
    public void testUpdateResponder() {

        Responder updateTo = new Responder.Builder("1").available(false).build();

        ResponderEntity updated = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();
        setField(updated, "id", 1);

        ResponderEntity updatedEntity = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();

        when(responderRepository.update(any(ResponderEntity.class))).thenReturn(ImmutableTriple.of(true, "success", updated));

        Triple<Boolean, String, Responder> result = responderService.updateResponder(updateTo);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(true));
        assertThat(result.getMiddle(), equalTo("success"));
        assertThat(result.getRight(), notNullValue());
        Responder updatedResponder = result.getRight();
        assertThat(updatedResponder.getId(), equalTo("1"));
        assertThat(updatedResponder.getName(), equalTo("John Doe"));
        assertThat(updatedResponder.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updatedResponder.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(updatedResponder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(updatedResponder.getBoatCapacity(), equalTo(3));
        assertThat(updatedResponder.isMedicalKit(), equalTo(true));
        assertThat(updatedResponder.isAvailable(), equalTo(false));
        assertThat(updatedResponder.isPerson(), equalTo(true));
        assertThat(updatedResponder.isEnrolled(), equalTo(true));

        verify(responderRepository).update(entityCaptor.capture());
        ResponderEntity entity = entityCaptor.getValue();
        assertThat(entity, notNullValue());
        assertThat(entity.getId(), equalTo(1L));
        assertThat(entity.getName(), nullValue());
        assertThat(entity.getPhoneNumber(), nullValue());
        assertThat(entity.getCurrentPositionLatitude(), nullValue());
        assertThat(entity.getCurrentPositionLongitude(), nullValue());
        assertThat(entity.getBoatCapacity(), nullValue());
        assertThat(entity.getMedicalKit(), nullValue());
        assertThat(entity.isAvailable(), equalTo(false));
        assertThat(entity.isPerson(), nullValue());
        assertThat(entity.isEnrolled(), nullValue());
    }

    @Test
    public void testUpdateResponderLocation() {

        Responder updateTo = new Responder.Builder("1")
                .latitude(new BigDecimal("30.98765")).longitude(new BigDecimal("-70.12345")).build();

        ResponderEntity current = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();
        setField(current, "id", 1);

        ResponderEntity updated = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.98765"))
                .currentPositionLongitude(new BigDecimal("-70.12345"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();
        setField(updated, "id", 1);

        when(responderRepository.findById(1L)).thenReturn(current);
        when(responderRepository.update(any(ResponderEntity.class))).thenReturn(ImmutableTriple.of(true, "success", updated));

        Triple<Boolean, String, Responder> result = responderService.updateResponderLocation(updateTo);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(true));
        assertThat(result.getMiddle(), equalTo("success"));
        assertThat(result.getRight(), notNullValue());
        Responder updatedResponder = result.getRight();
        assertThat(updatedResponder.getId(), equalTo("1"));
        assertThat(updatedResponder.getName(), equalTo("John Doe"));
        assertThat(updatedResponder.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updatedResponder.getLatitude(), equalTo(new BigDecimal("30.98765")));
        assertThat(updatedResponder.getLongitude(), equalTo(new BigDecimal("-70.12345")));
        assertThat(updatedResponder.getBoatCapacity(), equalTo(3));
        assertThat(updatedResponder.isMedicalKit(), equalTo(true));
        assertThat(updatedResponder.isAvailable(), equalTo(false));
        assertThat(updatedResponder.isPerson(), equalTo(true));
        assertThat(updatedResponder.isEnrolled(), equalTo(true));

        verify(responderRepository).update(entityCaptor.capture());
        ResponderEntity entity = entityCaptor.getValue();
        assertThat(entity, notNullValue());
        assertThat(entity.getId(), equalTo(1L));
        assertThat(entity.getName(), nullValue());
        assertThat(entity.getPhoneNumber(), nullValue());
        assertThat(entity.getCurrentPositionLatitude(), equalTo(new BigDecimal("30.98765")));
        assertThat(entity.getCurrentPositionLongitude(), equalTo(new BigDecimal("-70.12345")));
        assertThat(entity.getBoatCapacity(), nullValue());
        assertThat(entity.getMedicalKit(), nullValue());
        assertThat(entity.isAvailable(), nullValue());
        assertThat(entity.isPerson(), nullValue());
        assertThat(entity.isEnrolled(), nullValue());
    }

    @Test
    public void testUpdateResponderLocationWhenAvailable() {

        Responder updateTo = new Responder.Builder("1")
                .latitude(new BigDecimal("30.98765")).longitude(new BigDecimal("-70.12345")).build();

        ResponderEntity current = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();
        setField(current, "id", 1);

        when(responderRepository.findById(1L)).thenReturn(current);

        Triple<Boolean, String, Responder> result = responderService.updateResponderLocation(updateTo);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(false));
        assertThat(result.getRight(), notNullValue());
        Responder updatedResponder = result.getRight();
        assertThat(updatedResponder.getId(), equalTo("1"));
        assertThat(updatedResponder.getName(), equalTo("John Doe"));
        assertThat(updatedResponder.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updatedResponder.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(updatedResponder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(updatedResponder.getBoatCapacity(), equalTo(3));
        assertThat(updatedResponder.isMedicalKit(), equalTo(true));
        assertThat(updatedResponder.isAvailable(), equalTo(true));
        assertThat(updatedResponder.isPerson(), equalTo(true));
        assertThat(updatedResponder.isEnrolled(), equalTo(true));

        verify(responderRepository, never()).update(any(ResponderEntity.class));
    }

    @Test
    public void testUpdateResponderLocationWhenNotFound() {

        Responder updateTo = new Responder.Builder("1")
                .latitude(new BigDecimal("30.98765")).longitude(new BigDecimal("-70.12345")).build();

        when(responderRepository.findById(1L)).thenReturn(null);

        Triple<Boolean, String, Responder> result = responderService.updateResponderLocation(updateTo);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(false));
        assertThat(result.getRight(), nullValue());

        verify(responderRepository, never()).update(any(ResponderEntity.class));
    }

    @Test
    public void testReset() {
        responderService.reset();
        verify(responderRepository).reset();
    }

    @Test
    public void testClear() {

        ResponderEntity re1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        ResponderEntity re2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(re1, re2);

        when(responderRepository.nonPersonResponders()).thenReturn(responderEntities);

        responderService.clear(false);
        verify(responderRepository).nonPersonResponders();
        verify(responderRepository).clear();
        verify(eventPublisher).respondersDeleted(Arrays.asList(1L, 2L));
    }

    @Test
    public void testClearDeleteTrue() {

        ResponderEntity re1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        ResponderEntity re2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = Arrays.asList(re1, re2);

        when(responderRepository.nonPersonResponders()).thenReturn(responderEntities);

        responderService.clear(true);
        verify(responderRepository).nonPersonResponders();
        verify(responderRepository).resetPersonsDeleteBots();
        verify(eventPublisher).respondersDeleted(Arrays.asList(1L, 2L));
    }

    @Test
    public void testDeleteAll() {

        responderService.deleteAll();
        verify(responderRepository).deleteAll();
    }

    private void setField(Object targetObject, String name, Object value) {

        Class<?> targetClass = targetObject.getClass();
        Field field = findField(targetClass, name);
        if (field == null) {
            throw new IllegalArgumentException(String.format(
                    "Could not find field '%s' on %s", name, targetClass));
        }

        makeAccessible(field, targetObject);
        setField(field, targetObject, value);
    }

    private Field findField(Class<?> clazz, String name) {
        Class<?> searchType = clazz;
        while (Object.class != searchType && searchType != null) {
            Field[] fields = getDeclaredFields(searchType);
            for (Field field : fields) {
                if (name.equals(field.getName())) {
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    private Field[] getDeclaredFields(Class<?> clazz) {
        try {
            return clazz.getDeclaredFields();
        }
        catch (Throwable ex) {
            throw new IllegalStateException("Failed to introspect Class [" + clazz.getName() +
                    "] from ClassLoader [" + clazz.getClassLoader() + "]", ex);
        }
    }

    private void makeAccessible(Field field, Object object) {
        if ((!Modifier.isPublic(field.getModifiers()) ||
                !Modifier.isPublic(field.getDeclaringClass().getModifiers()) ||
                Modifier.isFinal(field.getModifiers())) && !field.canAccess(object)) {
            field.setAccessible(true);
        }
    }

    private void setField(Field field, Object target, Object value) {
        try {
            field.set(target, value);
        }
        catch (IllegalAccessException ex) {
            throw new IllegalStateException(
                    "Unexpected reflection exception - " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }
}
