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
package org.apache.plc4x.merlot.alarm.isa18_2.api;

import java.util.List;
import java.util.Map;
import org.apache.plc4x.merlot.alarm.isa18_2.model.Alarm;


public interface WidgetBehavior {

    /**
     *
     * @param newAlarm Standard procedure: As alarms occur, they are added one by one.
     */
    void addAlarm(Alarm newAlarm);

    /**
     *
     * @param blockAlarms Load a list of alarms to be displayed in the table.
     */
    void addBufferAlarm(List<Alarm> blockAlarms);

    /**
     *
     * @param messageFromRedis String retrieved from the Kafka broker
     * @return Alarm ready to be inserted into the table
     */
    Alarm parseAlarm(Map<String, String> body);

    /**
     * Clear the table—an option used when the widget starts up
     */
    void clearTable();

    /**
     * Close the connection to the alarm detection Kafka producer
     */
    void shutdown();
}
