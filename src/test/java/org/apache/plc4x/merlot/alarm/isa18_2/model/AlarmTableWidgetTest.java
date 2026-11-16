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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget.AlarmWidget.AlarmMode;
import org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget.AlarmWidgetRepresentation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AlarmTableWidgetTest {
     private static final Logger LOGGER = LoggerFactory.getLogger(AlarmTableWidgetTest.class);
    private AlarmTableWidget widget;
    
    @BeforeAll
    static void initJavaFxToolkit() throws InterruptedException {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            Platform.startup(latch::countDown);
            assertTrue(latch.await(5, TimeUnit.SECONDS), "The JavaFX toolkit could not be started");
        } catch (IllegalStateException ai) {
           LOGGER.info(ai.getLocalizedMessage());
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        // Empty broker to avoid real Kafka threads during testing
        runOnFxAndWait(() -> widget = new AlarmTableWidget("", "Test", "TestCommand"));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (widget != null) {
            runOnFxAndWait(() -> widget.shutdown());
        }
    }

    private static void runOnFxAndWait(Runnable action) throws Exception {
        if (Platform.isFxApplicationThread()) {
            action.run();
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        final Throwable[] error = new Throwable[1];
        Platform.runLater(() -> {
            try {
                action.run();
            } catch (Throwable t) {
                error[0] = t;
            } finally {
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timeout while waiting for the JavaFX thread");
        if (error[0] != null) {
            if (error[0] instanceof Exception e) {
                throw e;
            }
            throw new RuntimeException(error[0]);
        }
    }

    @SuppressWarnings("unchecked")
    private ObservableList<Alarm> getAlarmData() throws Exception {
        Field f = AlarmTableWidget.class.getDeclaredField("alarmData");
        f.setAccessible(true);
        return (ObservableList<Alarm>) f.get(widget);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> invokeToAlarmFields(String key, String json) throws Exception {
        Method m = AlarmTableWidget.class.getDeclaredMethod("toAlarmFields", String.class, String.class);
        m.setAccessible(true);
        return (Map<String, String>) m.invoke(widget, key, json);
    }

    @Test
    @DisplayName("It must correctly convert valid JSON from Phoebus into Alarm fields")
    void shouldParseValidPhoebusMessage() throws Exception {
        String key = "state:/Test/factory/p1/pva://demo.pressure";
        String json = """
                {
                  "severity": "MAJOR",
                  "message": "HIGH_PRESSURE",
                  "value": "85.456",
                  "time": {"seconds": 1730000000, "nano": 0},
                  "user": "",
                  "host": ""
                }
                """;

        Map<String, String> fields = invokeToAlarmFields(key, json);
        Alarm alarm = widget.parseAlarm(fields);

        assertNotNull(alarm);
        assertEquals("/Test/factory/p1/pva://demo.pressure", alarm.get("full_path"));
        assertEquals("factory/p1", alarm.getLocation());
        assertEquals("pva://demo.pressure", alarm.getPv());
        assertEquals("MAJOR", alarm.getSeverity());
        assertEquals("HIGH_PRESSURE", alarm.getMessage());
        assertEquals("85.46", alarm.get("value"));
    }

    @Test
    @DisplayName("You must discard messages without a valid PV protocol (sim://, pva://, loc://) or without severity")
    void shouldIgnoreInvalidMessages() throws Exception {
        String noSchemeKey = "state:/Test/factory/p1/invalid_pv";
        String validJson = "{\"severity\":\"MAJOR\",\"message\":\"ERR\"}";
        assertTrue(invokeToAlarmFields(noSchemeKey, validJson).isEmpty());

        String validKey = "state:/Test/factory/p1/pva://demo.pressure";
        String noSeverityJson = "{\"message\":\"ONLY_MSG\"}";
        assertTrue(invokeToAlarmFields(validKey, noSeverityJson).isEmpty());
    }

    @Test
    @DisplayName("RT mode should replace alarms for the same PV; HTC mode should stack them")
    void shouldDeduplicateInRtModeAndAppendInHtcMode() throws Exception {
        Alarm a1 = new Alarm(Map.of("pv", "pva://demo.pressure", "severity", "MINOR", "location", "factory/p1"));
        Alarm a2 = new Alarm(Map.of("pv", "pva://demo.pressure", "severity", "MAJOR", "location", "factory/p1"));

        runOnFxAndWait(() -> {
            widget.setAlarmMode(AlarmMode.RT);
            widget.addAlarm(a1);
            widget.addAlarm(a2);
        });

        ObservableList<Alarm> data = getAlarmData();
        assertEquals(1, data.size(), "In RT mode, there should be only 1 row per PV");
        assertEquals("MAJOR", data.get(0).getSeverity());

        runOnFxAndWait(() -> {
            widget.setAlarmMode(AlarmMode.HTC);
            widget.addAlarm(a1);
        });
        assertEquals(2, data.size(), "In HTC mode, all events must be accumulated");
    }

    @Test
    @DisplayName("You must not exceed the maximum limit of 100 rows in the table")
    void shouldEnforceMaxRowsLimit() throws Exception {
        runOnFxAndWait(() -> {
            widget.setAlarmMode(AlarmMode.HTC);
            for (int i = 0; i < 120; i++) {
                widget.addAlarm(new Alarm(Map.of("pv", "pva://pv_" + i, "severity", "MINOR")));
            }
        });
        assertEquals(100, getAlarmData().size());
    }

    @Test
    @DisplayName("The location filter must normalize bars and filter existing and incoming rows")
    void shouldFilterAlarmsByNormalizedLocationPrefix() throws Exception {
        Alarm p1 = new Alarm(Map.of("pv", "pva://pv1", "location", "factory/p1", "severity", "MAJOR"));
        Alarm p1Sub = new Alarm(Map.of("pv", "pva://pv2", "location", "factory/p1/lineA", "severity", "MINOR"));
        Alarm p2 = new Alarm(Map.of("pv", "pva://pv3", "location", "factory/p2", "severity", "MAJOR"));

        runOnFxAndWait(() -> {
            widget.addBufferAlarm(List.of(p1, p1Sub, p2));
            widget.setLocationFilter("/factory/p1/");
        });
        // Drain any task queued by runOnFx within setLocationFilter
        runOnFxAndWait(() -> {});

        assertEquals("factory/p1", widget.getLocationFilter());
        assertEquals(2, getAlarmData().size(), "You must keep factory/p1 and its subareas, and delete factory/p2");
    }

    @Test
    @DisplayName("acknowledgeAlarm  must update *_ACK locally and publish the command to Kafka")
    @SuppressWarnings("unchecked")
    void shouldAcknowledgeSingleAlarmAndPublishToKafka() throws Exception {
        KafkaProducer<String, String> mockProducer = mock(KafkaProducer.class);

        runOnFxAndWait(() -> {
            try {
                widget.reconfigure("127.0.0.1:9092", "Prueba", "PruebaCommand");
                Field producerField = AlarmTableWidget.class.getDeclaredField("commandProducer");
                producerField.setAccessible(true);
                producerField.set(widget, mockProducer);

                Alarm activeAlarm = new Alarm(Map.of(
                        "full_path", "/Test/factory/p1/pva://demo.pressure",
                        "location", "factory/p1",
                        "pv", "pva://demo.pressure",
                        "severity", "MAJOR"
                ));
                widget.addAlarm(activeAlarm);
                widget.acknowledgeAlarm(activeAlarm);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // 1. Check for immediate updates in the tablea
        assertEquals("MAJOR_ACK", getAlarmData().get(0).getSeverity());

        // 2. Check for asynchronous posting in the TestCommand thread
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, timeout(2000).times(1)).send(captor.capture(), any());

        ProducerRecord<String, String> sent = captor.getValue();
        assertEquals("PruebaCommand", sent.topic());
        assertEquals("command:/Test/factory/p1/pva://demo.pressure", sent.key());
        assertTrue(sent.value().contains("\"command\":\"acknowledge\""));
    }

    @Test
    @DisplayName("acknowledgeAllAlarms should acknowledge only active, unacknowledged alarms")
    @SuppressWarnings("unchecked")
    void shouldAcknowledgeOnlyActiveUnacknowledgedAlarms() throws Exception {
        KafkaProducer<String, String> mockProducer = mock(KafkaProducer.class);

        runOnFxAndWait(() -> {
            try {
                widget.reconfigure("127.0.0.1:9092", "Prueba", "");
                Field producerField = AlarmTableWidget.class.getDeclaredField("commandProducer");
                producerField.setAccessible(true);
                producerField.set(widget, mockProducer);

                Alarm a1 = new Alarm(Map.of("pv", "pva://pv1", "location", "factory/p1", "severity", "MAJOR"));
                Alarm a2 = new Alarm(Map.of("pv", "pva://pv2", "location", "factory/p1", "severity", "MINOR_ACK"));
                Alarm a3 = new Alarm(Map.of("pv", "pva://pv3", "location", "factory/p1", "severity", "OK"));
                Alarm a4 = new Alarm(Map.of("pv", "pva://pv4", "location", "factory/p1", "severity", "MINOR"));

                widget.addBufferAlarm(List.of(a1, a2, a3, a4));
                widget.acknowledgeAllAlarms();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Only pv1 (MAJOR) and pv4 (MINOR) should be submitted
        verify(mockProducer, timeout(2000).times(2)).send(any(ProducerRecord.class), any());
    }
}