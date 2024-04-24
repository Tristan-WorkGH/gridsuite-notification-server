/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.gridsuite.notification.server.TestUtils;
import org.gridsuite.notification.server.dto.study.Filters;
import org.gridsuite.notification.server.dto.study.FiltersToAdd;
import org.gridsuite.notification.server.dto.study.FiltersToRemove;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.gridsuite.notification.server.Utils.passHeader;
import static org.gridsuite.notification.server.handler.StudyNotificationWebSocketHandler.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@Slf4j
public class StudyNotificationWebSocketHandlerTest {

    private ObjectMapper objectMapper;
    private WebSocketSession ws;
    private WebSocketSession ws2;
    private HandshakeInfo handshakeinfo;

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Before
    public void setup() {
        objectMapper = new ObjectMapper().configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        var dataBufferFactory = new DefaultDataBufferFactory();

        ws = Mockito.mock(WebSocketSession.class);
        ws2 = Mockito.mock(WebSocketSession.class);
        handshakeinfo = Mockito.mock(HandshakeInfo.class);

        when(ws.getHandshakeInfo()).thenReturn(handshakeinfo);
        when(ws.receive()).thenReturn(Flux.empty());
        when(ws.send(any())).thenReturn(Mono.empty());
        when(ws.textMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String str = (String) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(str.getBytes()));
        });
        when(ws.pingMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Function<DataBufferFactory, DataBuffer> f = (Function<DataBufferFactory, DataBuffer>) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.PING, f.apply(dataBufferFactory));
        });
        when(ws.getId()).thenReturn("testsession");

        when(ws2.getHandshakeInfo()).thenReturn(handshakeinfo);
        when(ws2.send(any())).thenReturn(Mono.empty());
        when(ws2.textMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String str = (String) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(str.getBytes()));
        });
        when(ws2.pingMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Function<DataBufferFactory, DataBuffer> f = (Function<DataBufferFactory, DataBuffer>) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.PING, f.apply(dataBufferFactory));
        });
        when(ws2.getId()).thenReturn("testsession");
    }

    private void setUpUriComponentBuilder(String connectedUserId) {
        setUpUriComponentBuilder(connectedUserId, null, null);
    }

    private void setUpUriComponentBuilder(String connectedUserId, String filterStudyUuid, String filterUpdateType) {
        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HEADER_USER_ID, connectedUserId);
        when(handshakeinfo.getHeaders()).thenReturn(httpHeaders);

        if (filterStudyUuid != null) {
            uriComponentBuilder.queryParam(QUERY_STUDY_UUID, filterStudyUuid);
        }
        if (filterUpdateType != null) {
            uriComponentBuilder.queryParam(QUERY_UPDATE_TYPE, filterUpdateType);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
    }

    private void withFilters(String filterStudyUuid, String filterUpdateType, boolean inUrl) {
        String connectedUserId = "userId";
        String otherUserId = "userId2";

        Map<String, Object> filterMap = new HashMap<>();
        when(ws.getAttributes()).thenReturn(filterMap);

        if (inUrl) {
            setUpUriComponentBuilder(connectedUserId, filterStudyUuid, filterUpdateType);
        } else {
            setUpUriComponentBuilder(connectedUserId);
        }

        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(objectMapper, meterRegistry, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        var sink = atomicRef.get();
        notificationWebSocketHandler.handle(ws);

        if (!inUrl) {
            if (filterUpdateType != null) {
                filterMap.put(FILTER_UPDATE_TYPE, filterUpdateType);
            }
            if (filterStudyUuid != null) {
                filterMap.put(FILTER_STUDY_UUID, filterStudyUuid);
            }
        }

        List<GenericMessage<String>> refMessages = Stream.<Map<String, Object>>of(
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "oof"),

                Map.of(HEADER_STUDY_UUID, "foo bar/bar", HEADER_UPDATE_TYPE, "foobar"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "studies", HEADER_ERROR, "error_message"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab", HEADER_SUBSTATIONS_IDS, "s1"),

                Map.of(HEADER_STUDY_UUID, "public_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_ERROR, "error_message"),

                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "insert", HEADER_PARENT_NODE, UUID.randomUUID().toString(), HEADER_NEW_NODE, UUID.randomUUID().toString(), HEADER_INSERT_MODE, true),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "update", HEADER_NODES, List.of(UUID.randomUUID().toString())),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "update", HEADER_NODE, UUID.randomUUID().toString()),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "delete", HEADER_NODES, List.of(UUID.randomUUID().toString()),
                    HEADER_PARENT_NODE, UUID.randomUUID().toString(), HEADER_REMOVE_CHILDREN, true),

                Map.of(HEADER_STUDY_UUID, "", HEADER_UPDATE_TYPE, "indexation_status_updated", HEADER_INDEXATION_STATUS, "INDEXED"))
                .map(map -> new GenericMessage<>("", map))
                .toList();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        List<String> messages = new ArrayList<>();
        argument.getValue().map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);
        refMessages.forEach(sink::next);
        sink.complete();

        List<Map<String, Object>> expected = refMessages.stream()
            .filter(m -> {
                String studyUuid = (String) m.getHeaders().get(HEADER_STUDY_UUID);
                String updateType = (String) m.getHeaders().get(HEADER_UPDATE_TYPE);
                final boolean result = (filterStudyUuid == null || filterStudyUuid.equals(studyUuid))
                        && (filterUpdateType == null || filterUpdateType.equals(updateType));
                log.info("result = {}", result);
                return result;
            })
            .map(GenericMessage::getHeaders)
            .map(StudyNotificationWebSocketHandlerTest::toResultHeader)
            .toList();

        List<Map<String, Object>> actual = messages.stream().map(t -> {
            try {
                return toResultHeader(objectMapper.readValue(t, new TypeReference<Map<String, Map<String, Object>>>() { }).get("headers"));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        assertEquals(expected, actual);
        assertNotEquals(0, actual.size());
    }

    private static void handleReceivedFilters(WebSocketSession webSocketSession, Filters filters) {
        if (filters.filtersToRemove() != null) {
            FiltersToRemove filtersToRemove = filters.filtersToRemove();
            if (Boolean.TRUE.equals(filtersToRemove.removeUpdateType())) {
                webSocketSession.getAttributes().remove(FILTER_UPDATE_TYPE);
            }
            if (Boolean.TRUE.equals(filtersToRemove.removeStudyUuid())) {
                webSocketSession.getAttributes().remove(FILTER_STUDY_UUID);
            }
        }
        if (filters.filtersToAdd() != null) {
            FiltersToAdd filtersToAdd = filters.filtersToAdd();
            //because null is not allowed in ConcurrentHashMap and will cause the websocket to close
            if (filtersToAdd.updateType() != null) {
                webSocketSession.getAttributes().put(FILTER_UPDATE_TYPE, filtersToAdd.updateType());
            }
            if (filtersToAdd.studyUuid() != null) {
                webSocketSession.getAttributes().put(FILTER_STUDY_UUID, filtersToAdd.studyUuid());
            }
        }
    }

    private static Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        Map<String, Object> resHeader = new HashMap<>();
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));
        passHeader(messageHeader, resHeader, HEADER_STUDY_UUID);
        passHeader(messageHeader, resHeader, HEADER_ERROR);
        passHeader(messageHeader, resHeader, HEADER_SUBSTATIONS_IDS);
        passHeader(messageHeader, resHeader, HEADER_NEW_NODE);
        passHeader(messageHeader, resHeader, HEADER_NODE);
        passHeader(messageHeader, resHeader, HEADER_NODES);
        passHeader(messageHeader, resHeader, HEADER_REMOVE_CHILDREN);
        passHeader(messageHeader, resHeader, HEADER_PARENT_NODE);
        passHeader(messageHeader, resHeader, HEADER_INSERT_MODE);
        passHeader(messageHeader, resHeader, HEADER_INDEXATION_STATUS);
        return resHeader;
    }

    @Test
    public void testWithoutFilterInBody() {
        withFilters(null, null, false);
    }

    @Test
    public void testWithoutFilterInUrl() {
        withFilters(null, null, true);
    }

    @Test
    public void testStudyFilterInBody() {
        withFilters("bar", null, false);
    }

    @Test
    public void testStudyFilterInUrl() {
        withFilters("bar", null, true);
    }

    @Test
    public void testTypeFilterInBody() {
        withFilters(null, "rab", false);
    }

    @Test
    public void testTypeFilterInUrl() {
        withFilters(null, "rab", true);
    }

    @Test
    public void testStudyAndTypeFilterInBody() {
        withFilters("bar", "rab", false);
    }

    @Test
    public void testStudyAndTypeFilterInUrl() {
        withFilters("bar", "rab", true);
    }

    @Test
    public void testEncodingCharactersInBody() {
        withFilters("foo bar/bar", "foobar", false);
    }

    @Test
    public void testEncodingCharactersInUrl() {
        withFilters("foo bar/bar", "foobar", true);
    }

    @Test
    public void testWsReceiveFilters() throws JsonProcessingException {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        FiltersToAdd filtersToAdd = new FiltersToAdd("updateTypeFilter", "studyUuid");
        FiltersToRemove filtersToRemove = new FiltersToRemove(false, null);
        Filters filters = new Filters(filtersToAdd, filtersToRemove);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(new ObjectMapper(), meterRegistry, 60);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        TestUtils.receive(objectMapper, log, ws2, Filters.class, StudyNotificationWebSocketHandlerTest::handleReceivedFilters).subscribe();

        assertEquals("updateTypeFilter", map.get(FILTER_UPDATE_TYPE));
        assertEquals("studyUuid", map.get(FILTER_STUDY_UUID));
    }

    @Test
    public void testWsRemoveFilters() throws JsonProcessingException {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        map.put(FILTER_UPDATE_TYPE, "updateType");
        map.put(FILTER_STUDY_UUID, "studyUuid");
        FiltersToAdd filtersToAdd = new FiltersToAdd();
        FiltersToRemove filtersToRemove = new FiltersToRemove(true, true);
        Filters filters = new Filters(filtersToAdd, filtersToRemove);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        assertEquals("updateType", ws2.getAttributes().get(FILTER_UPDATE_TYPE));
        assertEquals("studyUuid", ws2.getAttributes().get(FILTER_STUDY_UUID));
        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(new ObjectMapper(), meterRegistry, Integer.MAX_VALUE);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        TestUtils.receive(objectMapper, log, ws2, Filters.class, StudyNotificationWebSocketHandlerTest::handleReceivedFilters).subscribe();

        assertNull(ws2.getAttributes().get(FILTER_UPDATE_TYPE));
        assertNull(ws2.getAttributes().get(FILTER_STUDY_UUID));
    }

    @Test
    public void testWsReceiveEmptyFilters() throws JsonProcessingException {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        Filters filters = new Filters();
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(new ObjectMapper(), meterRegistry, Integer.MAX_VALUE);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        TestUtils.receive(objectMapper, log, ws2, Filters.class, StudyNotificationWebSocketHandlerTest::handleReceivedFilters).subscribe();

        assertNull(map.get(FILTER_UPDATE_TYPE));
        assertNull(map.get(FILTER_STUDY_UUID));
    }

    @Test
    public void testWsReceiveUnprocessableFilter() {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap("UnprocessableFilter".getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(new ObjectMapper(), meterRegistry, 60);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        TestUtils.receive(objectMapper, log, ws2, Filters.class, StudyNotificationWebSocketHandlerTest::handleReceivedFilters).subscribe();

        assertNull(map.get(FILTER_UPDATE_TYPE));
        assertNull(map.get(FILTER_STUDY_UUID));
    }

    @Test
    public void testHeartbeat() {
        setUpUriComponentBuilder("userId");

        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(null, meterRegistry, 1);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        notificationWebSocketHandler.handle(ws);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        assertEquals("testsession-0", argument.getValue().blockFirst(Duration.ofSeconds(10)).getPayloadAsText());
    }

    @Test
    public void testDiscard() {
        setUpUriComponentBuilder("userId");

        var notificationWebSocketHandler = new StudyNotificationWebSocketHandler(objectMapper, meterRegistry, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeStudyNotification().accept(flux);
        var sink = atomicRef.get();
        Map<String, Object> headers = Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof");

        sink.next(new GenericMessage<>("", headers)); // should be discarded, no client connected

        notificationWebSocketHandler.handle(ws);

        ArgumentCaptor<Flux<WebSocketMessage>> argument1 = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument1.capture());
        List<String> messages1 = new ArrayList<>();
        Flux<WebSocketMessage> out1 = argument1.getValue();
        Disposable d1 = out1.map(WebSocketMessage::getPayloadAsText).subscribe(messages1::add);
        d1.dispose();

        sink.next(new GenericMessage<>("", headers)); // should be discarded, first client disconnected

        notificationWebSocketHandler.handle(ws);

        ArgumentCaptor<Flux<WebSocketMessage>> argument2 = ArgumentCaptor.forClass(Flux.class);
        verify(ws, times(2)).send(argument2.capture());
        List<String> messages2 = new ArrayList<>();
        Flux<WebSocketMessage> out2 = argument2.getValue();
        Disposable d2 = out2.map(WebSocketMessage::getPayloadAsText).subscribe(messages2::add);
        d2.dispose();

        sink.complete();
        assertEquals(0, messages1.size());
        assertEquals(0, messages2.size());
    }
}
