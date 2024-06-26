/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * <p>
 * A WebSocketHandler that sends messages from a broker to websockets opened by clients,
 * interleaving with pings to keep connections open.
 * </p><p>
 * Spring Cloud Stream gets the {@link #consumeMergeNotification} bean and calls it with the
 * flux from the broker. We call {@link Flux#publish() publish} and {@link ConnectableFlux#connect() connect}
 * to subscribe immediately to the flux and multicast the messages to all connected websockets
 * and to discard the messages when no websockets are connected.
 * </p>
 *
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@Component
public class MergeNotificationWebSocketHandler implements WebSocketHandler {

    private static final String CATEGORY_BROKER_INPUT = MergeNotificationWebSocketHandler.class.getName() + ".input-broker-messages";
    private static final String CATEGORY_WS_OUTPUT = MergeNotificationWebSocketHandler.class.getName() + ".output-websocket-messages";

    private static final String QUERY_PROCESS_UUID = "processUuid";
    private static final String QUERY_BUSINESS_PROCESS = "businessProcess";

    private static final String HEADER_TSO = "tso";
    private static final String HEADER_STATUS = "status";
    private static final String HEADER_DATE = "date";
    private static final String HEADER_PROCESS_UUID = "processUuid";
    private static final String HEADER_BUSINESS_PROCESS = "businessProcess";
    private static final String HEADER_ERROR = "error";
    private static final Set<String> HEADERS = Set.of(HEADER_TSO, HEADER_STATUS, HEADER_DATE, HEADER_PROCESS_UUID, HEADER_BUSINESS_PROCESS, HEADER_ERROR);

    private final ObjectMapper jacksonObjectMapper;
    private final int heartbeatInterval;
    private Flux<Message<String>> flux;

    public MergeNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
    }

    @Bean
    public Consumer<Flux<Message<String>>> consumeMergeNotification() {
        return f -> {
            ConnectableFlux<Message<String>> c = f.log(CATEGORY_BROKER_INPUT, Level.FINE).publish();
            this.flux = c;
            c.connect();
            // Force connect 1 fake subscriber to consumme messages as they come.
            // Otherwise, reactorcore buffers some messages (not until the connectable flux had
            // at least one subscriber. Is there a better way ?
            c.subscribe();
        };
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession, String processUuid, String businessProcess) {
        return flux.transform(f -> {
            Flux<Message<String>> res = f;
            if (processUuid != null && businessProcess != null) {
                res = res.filter(m -> processUuid.equals(m.getHeaders().get(HEADER_PROCESS_UUID)) &&
                        businessProcess.equals(m.getHeaders().get(HEADER_BUSINESS_PROCESS)));
            }
            return res;
        }).map(m -> {
            try {
                Map<String, Object> headers = m.getHeaders().entrySet()
                        .stream()
                        .filter(e -> HEADERS.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                Map<String, Object> submap = Map.of(
                        "payload", m.getPayload(),
                        "headers", headers);
                return jacksonObjectMapper.writeValueAsString(submap);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }).log(CATEGORY_WS_OUTPUT, Level.FINE).map(webSocketSession::textMessage);
    }

    /**
     * A heartbeat flux sending websockets pings
     */
    private Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        URI uri = webSocketSession.getHandshakeInfo().getUri();
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
        String processUuid = parameters.getFirst(QUERY_PROCESS_UUID);
        String businessProcess = parameters.getFirst(QUERY_BUSINESS_PROCESS);
        return webSocketSession
                .send(
                        notificationFlux(webSocketSession, processUuid, businessProcess)
                        .mergeWith(heartbeatFlux(webSocketSession)))
                .and(webSocketSession.receive());
    }
}
