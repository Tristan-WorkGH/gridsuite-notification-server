package org.gridsuite.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

import java.util.function.BiConsumer;

public final class TestUtils {
    private TestUtils() {
        throw new IllegalCallerException("Utility class haven't instance");
    }

    public static <F> Flux<WebSocketMessage> receive(ObjectMapper jacksonObjectMapper, Logger logger,
                                                     WebSocketSession webSocketSession, Class<F> filtersClass,
                                                     BiConsumer<WebSocketSession, F> handleReceivedFilters) {
        return webSocketSession.receive().doOnNext(webSocketMessage -> {
            try {
                //if it's not the heartbeat
                if (WebSocketMessage.Type.TEXT.equals(webSocketMessage.getType())) {
                    String wsPayload = webSocketMessage.getPayloadAsText();
                    logger.debug("Message received: session {} with {}", webSocketSession.getId(), wsPayload);
                    F receivedFilters = jacksonObjectMapper.readValue(webSocketMessage.getPayloadAsText(), filtersClass);
                    handleReceivedFilters.accept(webSocketSession, receivedFilters);
                }
            } catch (JsonProcessingException e) {
                logger.error("JSON reading error", e);
            }
        });
    }
}
