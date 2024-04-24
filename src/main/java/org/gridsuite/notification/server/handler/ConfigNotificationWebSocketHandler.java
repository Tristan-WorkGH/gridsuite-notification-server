/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server.handler;

import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.util.Strings;
import org.gridsuite.notification.server.exception.NotificationServerRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
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

/**
 * <p>
 * A WebSocketHandler that sends messages from a broker to websockets opened by clients,
 * interleaving with pings to keep connections open.
 * </p><p>
 * Spring Cloud Stream gets the {@link #consumeConfigNotification} bean and calls it with the
 * flux from the broker. We call {@link Flux#publish() publish} and {@link ConnectableFlux#connect() connect}
 * to subscribe immediately to the flux and multicast the messages to all connected websockets
 * and to discard the messages when no websockets are connected.
 * </p>
 *
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@Component
public class ConfigNotificationWebSocketHandler implements WebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNotificationWebSocketHandler.class);
    private static final String CATEGORY_BROKER_INPUT = ConfigNotificationWebSocketHandler.class.getName() + ".messages.input-broker";
    private static final String CATEGORY_WS_OUTPUT = ConfigNotificationWebSocketHandler.class.getName() + ".messages.output-websocket";
    public static final String HEADER_USER_ID = "userId";
    public static final String HEADER_APP_NAME = "appName";
    public static final String QUERY_APP_NAME = "appName";
    public static final String HEADER_PARAMETER_NAME = "parameterName";
    public static final String COMMON_APP_NAME = "common";

    private final ObjectMapper jacksonObjectMapper;
    private final int heartbeatInterval;
    private Flux<Message<String>> flux;

    public ConfigNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
    }

    @Bean
    public Consumer<Flux<Message<String>>> consumeConfigNotification() {
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
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession, String filterUserId, String filterAppName) {
        return flux.transform(f -> {
            Flux<Message<String>> res = f;
            res = res
                    .filter(m -> filterUserId.equals(m.getHeaders().get(HEADER_USER_ID)))
                    .filter(m -> filterAppName == null ||
                            filterAppName.equals(m.getHeaders().get(HEADER_APP_NAME)) ||
                            COMMON_APP_NAME.equals(m.getHeaders().get(HEADER_APP_NAME)));
            return res;
        }).map(m -> {
            try {
                Map<String, Object> headers = new HashMap<>();
                if (m.getHeaders().get(HEADER_APP_NAME) != null) {
                    headers.put(HEADER_APP_NAME, m.getHeaders().get(HEADER_APP_NAME));
                }
                if (m.getHeaders().get(HEADER_PARAMETER_NAME) != null) {
                    headers.put(HEADER_PARAMETER_NAME, m.getHeaders().get(HEADER_PARAMETER_NAME));
                }
                return jacksonObjectMapper.writeValueAsString(Map.of("payload", m.getPayload(), "headers", headers));
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
        HttpHeaders headers = webSocketSession.getHandshakeInfo().getHeaders();
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build(true).getQueryParams();
        String filterUserId = headers.getFirst(HEADER_USER_ID);
        String filterAppName = parameters.getFirst(QUERY_APP_NAME);

        if (Strings.isBlank(filterUserId)) {
            throw new NotificationServerRuntimeException(NotificationServerRuntimeException.NOT_ALLOWED);
        } else {
            filterUserId = URLDecoder.decode(filterUserId, StandardCharsets.UTF_8);
        }
        LOGGER.debug("New websocket connection for {}={} and {}={}", HEADER_USER_ID, filterUserId, QUERY_APP_NAME, filterAppName);
        return webSocketSession
                .send(notificationFlux(webSocketSession, filterUserId, filterAppName).mergeWith(heartbeatFlux(webSocketSession)))
                .and(webSocketSession.receive());
    }
}
