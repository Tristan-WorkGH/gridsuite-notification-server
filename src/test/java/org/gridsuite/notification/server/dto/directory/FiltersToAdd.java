/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server.dto.directory;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FiltersToAdd(String updateType, List<String> elementUuids) {
    public FiltersToAdd() {
        this(null, null);
    }
}