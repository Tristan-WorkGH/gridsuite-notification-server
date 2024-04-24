package org.gridsuite.notification.server;

import lombok.experimental.UtilityClass;

import java.util.Map;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@UtilityClass
public class Utils {
    public static void passHeader(Map<String, Object> messageHeader, Map<String, Object> resHeader, String headerName) {
        if (messageHeader.get(headerName) != null) {
            resHeader.put(headerName, messageHeader.get(headerName));
        }
    }
}
