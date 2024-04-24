/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.util.Strings;
import org.gridsuite.notification.server.exception.NotificationServerRuntimeException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

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
public class ConfigNotificationWebSocketHandler extends AbstractNotificationWebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNotificationWebSocketHandler.class);
    private static final String CATEGORY_BROKER_INPUT = ConfigNotificationWebSocketHandler.class.getName() + ".messages.input-broker";
    private static final String CATEGORY_WS_OUTPUT = ConfigNotificationWebSocketHandler.class.getName() + ".messages.output-websocket";
    public static final String HEADER_USER_ID = "userId";
    public static final String HEADER_APP_NAME = "appName";
    public static final String QUERY_APP_NAME = "appName";
    public static final String HEADER_PARAMETER_NAME = "parameterName";
    public static final String COMMON_APP_NAME = "common";

    private final ObjectMapper jacksonObjectMapper;

    public ConfigNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        super(heartbeatInterval);
        this.jacksonObjectMapper = jacksonObjectMapper;
    }

    @Bean
    public Consumer<Flux<Message<String>>> consumeConfigNotification() {
        return consumeMessage(CATEGORY_BROKER_INPUT);
    }

    private static String getUserId(final HttpHeaders headers) {
        final String userId = headers.getFirst(HEADER_USER_ID);
        if (Strings.isBlank(userId)) {
            throw new NotificationServerRuntimeException(NotificationServerRuntimeException.NOT_ALLOWED);
        } else {
            return URLDecoder.decode(userId, StandardCharsets.UTF_8);
        }
    }

    @NotNull
    @Override
    protected Flux<WebSocketMessage> notificationFlux(@NotNull final WebSocketSession webSocketSession) {
        URI uri = webSocketSession.getHandshakeInfo().getUri();
        HttpHeaders httpHeaders = webSocketSession.getHandshakeInfo().getHeaders();
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build(true).getQueryParams();
        String filterUserId = getUserId(httpHeaders);
        String filterAppName = parameters.getFirst(QUERY_APP_NAME);
        LOGGER.debug("New websocket connection for {} with {}={} and {}={}", uri, HEADER_USER_ID, filterUserId, QUERY_APP_NAME, filterAppName);

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
}
