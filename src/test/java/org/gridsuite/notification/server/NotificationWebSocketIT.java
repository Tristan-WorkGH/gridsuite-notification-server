/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.gridsuite.notification.server.handler.ConfigNotificationWebSocketHandler;
import org.gridsuite.notification.server.handler.StudyNotificationWebSocketHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.StandardWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, classes = { NotificationApplication.class })
@DirtiesContext
public class NotificationWebSocketIT {
    @LocalServerPort
    private String port;

    @Autowired
    private MeterRegistry meterRegistry;

    private URI getUrl(String path) {
        return URI.create("ws://localhost:" + this.port + path);
    }

    private void testMeter(String name, int val) {
        Gauge meter = meterRegistry.get(name).gauge();
        assertNotNull(meter);
        assertEquals(val, Double.valueOf(meter.value()).intValue());
    }

    @Test
    public void configEcho() {
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        HttpHeaders headers = new HttpHeaders();
        headers.add(ConfigNotificationWebSocketHandler.HEADER_USER_ID, "testId");
        client.execute(getUrl("/config/notify"), headers, WebSocketSession::close).block();
    }

    @Test
    public void configEcho2() {
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        //HttpHeaders headers = new HttpHeaders();
        //headers.add("userId", "testId");
        client.execute(getUrl("/config/global"), /*headers,*/ WebSocketSession::close).block();
    }

    @Test
    public void directoryEcho() {
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        client.execute(getUrl("/directory/notify"), WebSocketSession::close).block();
    }

    @Test
    public void mergeEcho() {
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        client.execute(getUrl("/merge/notify"), WebSocketSession::close).block();
    }

    @Test
    public void studyEcho() {
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(StudyNotificationWebSocketHandler.HEADER_USER_ID, "testId");
        client.execute(getUrl("/study/notify"), httpHeaders, ws -> Mono.empty()).block();
    }

    @Test
    public void studyMetrics() {
        testStudyMeters(0);
        WebSocketClient client = new StandardWebSocketClient();
        assertNotNull(client);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(StudyNotificationWebSocketHandler.HEADER_USER_ID, "testId");
        client.execute(getUrl("/study/notify"), httpHeaders, ws -> Mono.fromRunnable(() -> testStudyMeters(1))).block();
    }

    private void testStudyMeters(int val) {
        testMeter(StudyNotificationWebSocketHandler.USERS_METER_NAME, val);
        testMeter(StudyNotificationWebSocketHandler.CONNECTIONS_METER_NAME, val);
    }
}
