package org.gridsuite.notification.server.handler;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.messaging.Message;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * <p>
 * The base of a WebSocketHandler that sends messages from a broker to websockets opened by clients,
 * interleaving with pings to keep connections open.
 * </p><p>
 * Spring Cloud Stream gets the {@link #consumeMessage} bean and calls it with the
 * flux from the broker. We call {@link Flux#publish() publish} and {@link reactor.core.publisher.ConnectableFlux#connect() connect}
 * to subscribe immediately to the flux and multicast the messages to all connected websockets
 * and to discard the messages when no websockets are connected.
 * </p>
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractNotificationWebSocketHandler implements WebSocketHandler {
    private final int heartbeatInterval;
    protected Flux<Message<String>> flux;

    @NotNull
    protected Consumer<Flux<Message<String>>> consumeMessage(@NotNull String logCategory) {
        return f -> {
            ConnectableFlux<Message<String>> c = f.log(logCategory, Level.FINE).publish();
            this.flux = c;
            c.connect();
            /* Force connect 1 fake subscriber to consume messages as they come.
             * Otherwise, reactor-core buffers some messages (not until the connectable flux had
             * at least one subscriber. Is there a better way?
             */
            c.subscribe();
        };
    }

    /**
     * Map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    @NotNull
    protected abstract Flux<WebSocketMessage> notificationFlux(@NotNull final WebSocketSession webSocketSession);

    /**
     * A heartbeat flux sending websockets pings
     */
    @NotNull
    private Flux<WebSocketMessage> heartbeatFlux(@NotNull final WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval))
            .map(n -> webSocketSession.pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public Mono<Void> handle(@NotNull final WebSocketSession webSocketSession) {
        return webSocketSession
                .send(notificationFlux(webSocketSession).mergeWith(heartbeatFlux(webSocketSession)))
                .and(webSocketSession.receive());
    }
}
