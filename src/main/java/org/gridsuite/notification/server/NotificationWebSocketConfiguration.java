/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server;

import org.gridsuite.notification.server.config.ConfigGlobalNotificationWebSocketHandler;
import org.gridsuite.notification.server.config.ConfigNotificationWebSocketHandler;
import org.gridsuite.notification.server.directory.DirectoryNotificationWebSocketHandler;
import org.gridsuite.notification.server.merge.MergeNotificationWebSocketHandler;
import org.gridsuite.notification.server.study.StudyNotificationWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.Map;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@Configuration
public class NotificationWebSocketConfiguration {
    @Bean
    public HandlerMapping webSocketHandlerMapping(ConfigNotificationWebSocketHandler configWebSocketHandler,
                                                  ConfigGlobalNotificationWebSocketHandler configGlobalWebSocketHandler,
                                                  DirectoryNotificationWebSocketHandler directoryWebSocketHandler,
                                                  MergeNotificationWebSocketHandler mergeWebSocketHandler,
                                                  StudyNotificationWebSocketHandler studyWebSocketHandler) {
        return new SimpleUrlHandlerMapping(Map.of(
            "/config/notify", configWebSocketHandler,
            "/config/global", configGlobalWebSocketHandler,
            "/directory/notify", directoryWebSocketHandler,
            "/merge/notify", mergeWebSocketHandler,
            "/study/notify", studyWebSocketHandler
        ), 1);
    }

    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
