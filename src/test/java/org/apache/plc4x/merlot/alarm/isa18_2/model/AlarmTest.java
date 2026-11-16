/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.plc4x.merlot.alarm.isa18_2.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AlarmTest {

    @Test
    @DisplayName("It must return the expected fields and an empty string for keys that do not exist.")
    void shouldReturnExpectedFieldsAndEmptyDefault() {
        Map<String, String> input = new HashMap<>();
        input.put("location", "factory/p1");
        input.put("pv", "pva://demo.pressure");
        input.put("severity", "MAJOR");
        input.put("message", "HIGH");

        Alarm alarm = new Alarm(input);

        assertEquals("factory/p1", alarm.getLocation());
        assertEquals("pva://demo.pressure", alarm.getPv());
        assertEquals("MAJOR", alarm.getSeverity());
        assertEquals("HIGH", alarm.getMessage());
        assertEquals("", alarm.get("non_existent_key"));
    }

    @Test
    @DisplayName("You must ensure the immutability of the internal field map")
    void shouldEnsureImmutabilityOfFields() {
        Map<String, String> input = new HashMap<>();
        input.put("severity", "MINOR");

        Alarm alarm = new Alarm(input);
        //Forced change
        input.put("severity", "MAJOR");

        assertEquals("MINOR", alarm.getSeverity());
        assertThrows(UnsupportedOperationException.class, () -> alarm.getFields().put("user", "luis"));
    }
}