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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.plc4x.merlot.alarm.isa18_2.api.WidgetBehavior;
import org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget.AlarmWidget.AlarmMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmTableWidget extends StackPane implements WidgetBehavior {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmTableWidget.class);
    private static final int MAX_ROWS = 100;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    //Verifies that the route contains a complete PV with the sim://, pva://, or loc:// protocol
    private static final Pattern PV_SCHEME_PATTERN =
            Pattern.compile("(sim|pva|loc)://.+", Pattern.CASE_INSENSITIVE);

    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final TableView<Alarm> alarmTable;
    private final ObservableList<Alarm> alarmData;

    // Current Parameters
    private String bootstrapServers;
    private String topic;
    private String commandTopic;
    private volatile String locationFilter = "";
    private volatile AlarmMode mode = AlarmMode.RT;

    // Kafka Consumer and Producer Session
    private ConsumerSession session;
    private KafkaProducer<String, String> commandProducer;

    private boolean attached = false;
    private boolean disposed = false;

    // Style configurable from the Phoebus widget properties
    private Color headerBackground;
    private Color headerForeground;
    private Font headerFont;
    private Color tableBackground;
    private Font tableFont;

    // Configurable colors based on severity
    private Color majorColor = Color.web("#FF0000");
    private Color minorColor = Color.web("#FFFF00");
    private Color invalidColor = Color.web("#333333");
    private Color okColor = Color.web("#90EE90");

    public AlarmTableWidget(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, "");
    }

    public AlarmTableWidget(String bootstrapServers, String topic, String commandTopic) {
        setMinSize(0, 0);
        setMaxSize(Double.MAX_VALUE, Double.MAX_VALUE);

        // 1. Observable list, table contents
        alarmData = FXCollections.observableArrayList();

        // 2. Table with no fixed size: adapts to the StackPane container
        alarmTable = new TableView<>();
        alarmTable.setItems(alarmData);
        alarmTable.setMinSize(0, 0);
        alarmTable.setPrefSize(USE_COMPUTED_SIZE, USE_COMPUTED_SIZE);
        alarmTable.setMaxSize(Double.MAX_VALUE, Double.MAX_VALUE);
        alarmTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);

        // Header nodes exist only when the table has a skin
        alarmTable.skinProperty().addListener((obs, oldSkin, newSkin) -> applyHeaderStyle());

        // Table columns
        alarmTable.getColumns().add(column("Date/Time", "datetime", 140));
        alarmTable.getColumns().add(column("Severity", "severity", 85));
        alarmTable.getColumns().add(column("Location", "location", 160));
        alarmTable.getColumns().add(column("PV", "pv", 130));
        alarmTable.getColumns().add(column("Message", "message", 180));
        alarmTable.getColumns().add(column("Value", "value", 70));
        alarmTable.getColumns().add(column("User", "user", 80));
        alarmTable.getColumns().add(column("Host", "host", 130));

        // 3. Row colors based on severity + context menu (“Acknowledge This” / “Acknowledge All”)
        alarmTable.setRowFactory(tv -> {
            TableRow<Alarm> row = new TableRow<>() {
                @Override
                protected void updateItem(Alarm item, boolean empty) {
                    super.updateItem(item, empty);
                    if (item == null || empty) {
                        setStyle("");
                    } else {
                        Color rowColor = resolveSeverityColor(item.getSeverity());
                        setStyle(buildRowCss(rowColor));
                    }
                }
            };

            // Ensure that right-clicking selects the row before opening the ContextMenu
            row.setOnMousePressed(e -> {
                if (!row.isEmpty()) {
                    alarmTable.getSelectionModel().select(row.getIndex());
                }
            });

            ContextMenu contextMenu = new ContextMenu();

            MenuItem ackThisItem = new MenuItem("Acknowledge This");
            ackThisItem.setOnAction(evt -> {
                Alarm selected = alarmTable.getSelectionModel().getSelectedItem();
                if (selected == null) {
                    selected = row.getItem();
                }
                if (selected != null) {
                    acknowledgeAlarm(selected);
                }
            });

            MenuItem ackAllItem = new MenuItem("Acknowledge All");
            ackAllItem.setOnAction(evt -> acknowledgeAllAlarms());

            contextMenu.getItems().addAll(ackThisItem, ackAllItem);

            // Show the context menu only when the row contains a valid alarm
            row.contextMenuProperty().bind(
                    Bindings.when(row.emptyProperty())
                            .then((ContextMenu) null)
                            .otherwise(contextMenu)
            );

            // Dynamically enable each option based on the current status
            contextMenu.setOnShowing(evt -> {
                Alarm item = alarmTable.getSelectionModel().getSelectedItem();
                if (item == null) {
                    item = row.getItem();
                }
                boolean canAckThis = item != null && canAcknowledge(item.getSeverity());
                boolean canAckAny = alarmData.stream().anyMatch(a -> canAcknowledge(a.getSeverity()));

                ackThisItem.setDisable(!canAckThis);
                ackAllItem.setDisable(!canAckAny);
            });

            return row;
        });

        // 4. Add the table
        getChildren().add(alarmTable);

        sceneProperty().addListener((obs, oldScene, newScene) -> setAttached(newScene != null));

        reconfigure(bootstrapServers, topic, commandTopic);
    }

   /**
    * Location Filter
    * @param filter Area/location path prefix used to filter which alarms are displayed in the table
    */
    public void setLocationFilter(String filter) {
        String normalized = normalizeLocationPrefix(filter);
        if (!Objects.equals(this.locationFilter, normalized)) {
            this.locationFilter = normalized;
            runOnFx(this::applyLocationFilterToExistingRows);
        }
    }

    public String getLocationFilter() {
        return locationFilter;
    }

    
    private static String normalizeLocationPrefix(String raw) {
        if (raw == null) {
            return "";
        }
        String trimmed = raw.trim();
        while (trimmed.startsWith("/")) {
            trimmed = trimmed.substring(1);
        }
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    /**
     * 
     * @param formattedLocation Standardized text that should be compared to the text entered in the widget
     * @return true if they match; otherwise, false
     */
    private boolean matchesLocationFilter(String formattedLocation) {
        String filter = this.locationFilter;
        if (filter == null || filter.isEmpty()) {
            return true;
        }
        if (formattedLocation == null || formattedLocation.isEmpty()) {
            return false;
        }
        return formattedLocation.equals(filter)
                || formattedLocation.startsWith(filter + "/");
    }

    /**
     * Remove from the list that populates the table any alarms that do not match the area filter
     */
    private void applyLocationFilterToExistingRows() {
        String filter = this.locationFilter;
        if (filter == null || filter.isEmpty()) {
            return;
        }
        alarmData.removeIf(alarm -> !matchesLocationFilter(alarm.getLocation()));
        alarmTable.refresh();
    }

   /**
    * Acknowledgment to Phoebus Alarm Server (Kafka Command Topic)
    * @param severity Current alarm status other than OK/NORMAL
    * @return true if the alarm is active and unacknowledged (can be acknowledged); otherwise, false
    */
    private static boolean canAcknowledge(String severity) {
        if (severity == null || severity.isBlank()) {
            return false;
        }
        String upper = severity.trim().toUpperCase(Locale.ROOT);
        return !upper.endsWith("_ACK")
                && !"OK".equals(upper)
                && !"NO_ALARM".equals(upper);
    }

    /**
     * Acknowledge an individual alarm (“Acknowledge This”).
     */
    public void acknowledgeAlarm(Alarm alarm) {
        if (alarm == null || !canAcknowledge(alarm.getSeverity())) {
            return;
        }
        acknowledgeBatch(Collections.singletonList(alarm));
    }

    /**
     * Acknowledges all active alarms that have not been acknowledged in the table (“Acknowledge All”).
     */
    public void acknowledgeAllAlarms() {
        List<Alarm> pending = new ArrayList<>();
        for (Alarm alarm : alarmData) {
            if (alarm != null && canAcknowledge(alarm.getSeverity())) {
                pending.add(alarm);
            }
        }
        if (!pending.isEmpty()) {
            acknowledgeBatch(pending);
        }
    }

    /**
     * Verify that the topic used in Kafka ends with “Command”; if it does not, add it.
     * @return Text to be used as the topic name for alarm recognition
     */
    private String resolveCommandTopic() {
        if (commandTopic != null && !commandTopic.isBlank()) {
            return commandTopic.trim();
        }
        if (topic != null && !topic.isBlank()) {
            return topic.endsWith("Command") ? topic : topic + "Command";
        }
        return "";
    }

   
    /**
     * Sends “acknowledge” commands to the topic configured in the UI 
     * (or “<topic>Command”) and updates its visual status to *_ACK.
     * @param alarmsToAck List of alarms that should be sent to the acknowledgment thread
     */
    private void acknowledgeBatch(List<Alarm> alarmsToAck) {
        final String targetCommandTopic = resolveCommandTopic();
        if (alarmsToAck == null || alarmsToAck.isEmpty()
                || bootstrapServers == null || bootstrapServers.isEmpty()
                || targetCommandTopic.isEmpty()) {
            return;
        }

        final String user = System.getProperty("user.name", "operator");
        final String host = resolveHostName();

        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("user", user);
        payload.put("host", host);
        payload.put("command", "acknowledge");

        final String jsonPayload;
        try {
            jsonPayload = MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            LOGGER.info("Error serializing the acknowledge command: {}",e.getMessage());
            return;
        }

        List<String> commandKeys = new ArrayList<>();

        for (Alarm alarm : alarmsToAck) {
            if (!canAcknowledge(alarm.getSeverity())) {
                continue;
            }

            String fullPath = alarm.get("full_path");
            if (fullPath == null || fullPath.isEmpty()) {
                String loc = normalizeLocationPrefix(alarm.getLocation());
                String pv = alarm.getPv();
                String rootTopic = normalizeLocationPrefix(topic);
                if (loc.isEmpty()) {
                    fullPath = "/" + rootTopic + "/" + pv;
                } else {
                    fullPath = "/" + rootTopic + "/" + loc + "/" + pv;
                }
            }

            String commandKey = "command:" + fullPath;
            if (!commandKeys.contains(commandKey)) {
                commandKeys.add(commandKey);
            }

            // Immediate visual update in the queue
            Map<String, String> updatedFields = new HashMap<>(alarm.getFields());
            updatedFields.put("severity", alarm.getSeverity() + "_ACK");
            updatedFields.put("user", user);
            updatedFields.put("host", host);
            Alarm ackedLocal = new Alarm(updatedFields);

            int idx = alarmData.indexOf(alarm);
            if (idx >= 0) {
                alarmData.set(idx, ackedLocal);
            }
        }

        alarmTable.refresh();

        if (commandKeys.isEmpty()) {
            return;
        }

        // Asynchronous Publishing in Kafka
        Thread sender = new Thread(() -> {
            try {
                KafkaProducer<String, String> producer = getOrCreateProducer();
                if (producer != null) {
                    for (String key : commandKeys) {
                        producer.send(new ProducerRecord<>(targetCommandTopic, key, jsonPayload), (meta, ex) -> {
                            if (ex != null) {
                                LOGGER.info("Error sending acknowledgment ({}) a Kafka ({}): {}", key, targetCommandTopic, ex.getMessage());
                            }
                        });
                    }
                    producer.flush();
                }
            } catch (Exception e) {
                LOGGER.info("Error when publishing “acknowledge” to Kafka: {}", e.getMessage());
            }
        }, "kafka-alarm-ack");
        sender.setDaemon(true);
        sender.start();
    }

    /**
     * 
     * @return Kafka producer: if it doesn't exist, it creates it and returns it
     */
    private synchronized KafkaProducer<String, String> getOrCreateProducer() {
        if (disposed || bootstrapServers == null || bootstrapServers.isEmpty()) {
            return null;
        }
        if (commandProducer == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "alarm-table-cmd-" + UUID.randomUUID());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            commandProducer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        }
        return commandProducer;
    }

    /**
     * The Kafka producer closes
     */
    private synchronized void closeProducer() {
        if (commandProducer != null) {
            try {
                commandProducer.close(Duration.ofMillis(500));
            } catch (Exception ignored) {
            } finally {
                commandProducer = null;
            }
        }
    }

    /**
     * 
     * @return Host of the machine on which Phoebus is running
     */
    private static String resolveHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

 /**
  * Mode RT / HTC
  * @param newMode Sets the operating mode of the alarm table
  ** RT: If an incoming alarm is received, the system checks to see if there is 
  * a record that matches the PV; if there is a match, the parameters are updated. 
  * If the alarm has a latch, its status in the table will not be updated until 
  * it is acknowledged.
  ** HTC: Add the alarms to the table as they appear, even if there are already
  * records in the table.
  */
    public void setAlarmMode(AlarmMode newMode) {
        AlarmMode targetMode = (newMode == null) ? AlarmMode.RT : newMode;
        if (this.mode != targetMode) {
            this.mode = targetMode;
            if (this.mode == AlarmMode.RT) {
                runOnFx(this::deduplicateForRt);
            }
        }
    }

    public AlarmMode getAlarmMode() {
        return mode;
    }

    /**
     * Uniqueness Validation for RT Mode
     */
    private void deduplicateForRt() {
        List<Alarm> unique = new ArrayList<>();
        for (Alarm a : alarmData) {
            boolean exists = unique.stream().anyMatch(u -> isSamePv(u, a));
            if (!exists) {
                unique.add(a);
            }
        }
        alarmData.setAll(unique);
    }

    /**
     * 
     * @param a alarm temporarily stored in the uniqueness list for RT
     * @param b alarm saved in the list used to populate the table
     * @return true if both alarms belong to the same PV; otherwise, false
     */
    private static boolean isSamePv(Alarm a, Alarm b) {
        if (a == null || b == null) {
            return false;
        }
        String pvA = a.getPv() == null ? "" : a.getPv().trim();
        String pvB = b.getPv() == null ? "" : b.getPv().trim();
        return !pvA.isEmpty() && Objects.equals(pvA, pvB);
    }

   /**
    * 
    * @param background the background color style that the widget will use in Phoebus
    * @param foreground font color style that the widget will use in Phoebus
    */
    public void setHeaderColors(Color background, Color foreground) {
        headerBackground = background;
        headerForeground = foreground;
        applyHeaderStyle();
    }

    /**
     * 
     * @param major background color to be used to highlight the MAJOR-level alarm
     * @param minor background color to be used to highlight the alarm with a MINOR severity
     * @param invalid background color to be used to highlight the alarm with a INVALID severity
     * @param ok background color to be used to highlight the alarm with a OK/NORMAL severity
     */
    public void setSeverityColors(Color major, Color minor, Color invalid, Color ok) {
        if (major != null)   this.majorColor = major;
        if (minor != null)   this.minorColor = minor;
        if (invalid != null) this.invalidColor = invalid;
        if (ok != null)      this.okColor = ok;
        runOnFx(alarmTable::refresh);
    }

    /**
     * 
     * @param severity Alarm Severity Level
     * @return Background color associated with that severity level
     */
    private Color resolveSeverityColor(String severity) {
        switch (baseSeverity(severity).toUpperCase(Locale.ROOT)) {
            case "MAJOR":
                return majorColor;
            case "MINOR":
                return minorColor;
            case "INVALID":
            case "UNDEFINED":
                return invalidColor;
            default:
                return okColor;
        }
    }

    /**
     * 
     * @param bg Color to format the row
     * @return row formatted with the color style
     */
    private static String buildRowCss(Color bg) {
        Color c = (bg == null) ? Color.WHITE : bg;
        double luminance = 0.299 * c.getRed() + 0.587 * c.getGreen() + 0.114 * c.getBlue();
        String textColor = (luminance < 0.5) ? "white" : "black";
        return "-fx-background-color: " + toRgba(c) + "; -fx-text-background-color: " + textColor + ";";
    }

    /**
     * 
     * @param font Customizing the Font in the Table Header
     */
    public void setHeaderFont(Font font) {
        headerFont = font;
        applyHeaderStyle();
    }

    /**
     * 
     * @param background Table background color
     */
    public void setTableBackground(Color background) {
        tableBackground = background;
        refreshTableStyle();
    }

    /**
     * 
     * @param font Customizing the Font of Table Rows
     */
    public void setTableFont(Font font) {
        tableFont = font;
        refreshTableStyle();
        applyHeaderStyle();
    }

    /**
     * 
     * @param f JavaFX Font object to convert into an inline CSS string
     * @return CSS style sheet
     */
    private static String fontToCss(Font f) {
        if (f == null) {
            return "";
        }
        StringBuilder css = new StringBuilder();
        css.append("-fx-font-family: \"").append(f.getFamily()).append("\";");
        css.append("-fx-font-size: ").append(f.getSize()).append("px;");
        String style = f.getStyle() == null ? "" : f.getStyle();
        if (style.contains("Bold")) {
            css.append("-fx-font-weight: bold;");
        } else {
            css.append("-fx-font-weight: normal;");
        }
        if (style.contains("Italic")) {
            css.append("-fx-font-style: italic;");
        } else {
            css.append("-fx-font-style: normal;");
        }
        return css.toString();
    }

    private void refreshTableStyle() {
        StringBuilder css = new StringBuilder();
        if (tableBackground != null) {
            css.append("-fx-control-inner-background: ").append(toWeb(tableBackground)).append(";");
        }
        css.append(fontToCss(tableFont));
        try {
            alarmTable.setStyle(css.toString());
        } catch (Exception e) {
            LOGGER.info("Error applying style to table [{}]:  {}", css,  e.getMessage());
            alarmTable.setStyle("");
        }
    }

    private void applyHeaderStyle() {
        Platform.runLater(() -> {
            try {
                String bg = headerBackground == null
                        ? "" : "-fx-background-color: " + toWeb(headerBackground) + ";";
                String fg = headerForeground == null
                        ? "" : "-fx-text-fill: " + toWeb(headerForeground) + ";";
                String fnt = fontToCss(headerFont);

                for (Node n : alarmTable.lookupAll(".column-header-background")) {
                    n.setStyle(bg);
                }
                for (Node n : alarmTable.lookupAll(".filler")) {
                    n.setStyle(bg);
                }
                for (Node n : alarmTable.lookupAll(".column-header")) {
                    n.setStyle(bg);
                    for (Node label : n.lookupAll(".label")) {
                        label.setStyle(fg + fnt);
                    }
                }
            } catch (Exception e) {
                LOGGER.info("Error applying style to the header: {}", e);
            }
        });
    }

    private static String toWeb(Color c) {
        return String.format("#%02X%02X%02X",
                Math.round(c.getRed() * 255),
                Math.round(c.getGreen() * 255),
                Math.round(c.getBlue() * 255));
    }

    private static String toRgba(Color c) {
        return String.format(Locale.ROOT, "rgba(%d, %d, %d, %.3f)",
                Math.round(c.getRed() * 255),
                Math.round(c.getGreen() * 255),
                Math.round(c.getBlue() * 255),
                c.getOpacity());
    }

    // Protection Against Null Values in JavaFX Cells (Prevents NullPointerException in the FX Thread)
    private TableColumn<Alarm, String> column(String title, String key, double width) {
        TableColumn<Alarm, String> col = new TableColumn<>(title);
        col.setCellValueFactory(cd -> {
            Alarm a = cd.getValue();
            String val = (a == null) ? "" : a.get(key);
            return new ReadOnlyStringWrapper(val == null ? "" : val);
        });
        col.setPrefWidth(width);
        return col;
    }

   /**
    * Reconfigures Kafka connection parameters from the UI
    * @param newBootstrapServers Kafka broker host
    * @param newTopic A Kafka topic that will be used to consume messages
    */
    public synchronized void reconfigure(String newBootstrapServers, String newTopic) {
        reconfigure(newBootstrapServers, newTopic, commandTopic);
    }

    /**
     * 
     * @param newBootstrapServers A new Kafka broker will be set up for your reference
     * @param newTopic A new Kafka topic will be created for your reference
     * @param newCommandTopic New Kafka command topic to be created for reference
     */
    public synchronized void reconfigure(String newBootstrapServers, String newTopic, String newCommandTopic) {
        String servers = newBootstrapServers == null ? "" : newBootstrapServers.trim();
        String top = newTopic == null ? "" : newTopic.trim();
        String cmdTop = newCommandTopic == null ? "" : newCommandTopic.trim();

        boolean consumerChanged = !servers.equals(bootstrapServers) || !top.equals(topic);
        boolean producerChanged = !servers.equals(bootstrapServers) || !cmdTop.equals(commandTopic);

        if (!consumerChanged && !producerChanged) {
            return;
        }

        bootstrapServers = servers;
        topic = top;
        commandTopic = cmdTop;

        if (producerChanged) {
            closeProducer();
        }
        if (consumerChanged) {
            runOnFx(this::clearTable);
            restartSession();
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getCommandTopic() {
        return commandTopic;
    }

    private synchronized void setAttached(boolean value) {
        attached = value;
        restartSession();
    }

    private synchronized void restartSession() {
        stopSession();

        if (disposed || !attached
                || bootstrapServers == null || bootstrapServers.isEmpty()
                || topic == null || topic.isEmpty()) {
            return;
        }

        session = new ConsumerSession(bootstrapServers, topic);
        Thread t = new Thread(session, "kafka-alarm-consumer");
        t.setDaemon(true);
        t.start();
    }

    private void stopSession() {
        if (session != null) {
            session.stop();
            session = null;
        }
    }

 /**
  * Kafka Consumer Task
  */
    private final class ConsumerSession implements Runnable {

        private final String servers;
        private final String topicName;
        private volatile boolean running = true;
        private volatile KafkaConsumer<String, String> consumer;

        ConsumerSession(String servers, String topicName) {
            this.servers = servers;
            this.topicName = topicName;
        }

        void stop() {
            running = false;
            KafkaConsumer<String, String> c = consumer;
            if (c != null) {
                c.wakeup();
            }
        }

        private KafkaConsumer<String, String> createConsumer() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "alarm-table-" + UUID.randomUUID());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
            return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        }

        @Override
        public void run() {
            while (running) {
                try (KafkaConsumer<String, String> c = createConsumer()) {
                    consumer = c;
                    c.subscribe(Collections.singletonList(topicName));

                    while (running) {
                        ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
                        if (records.isEmpty()) {
                            continue;
                        }

                        List<Alarm> batch = new ArrayList<>();
                        for (ConsumerRecord<String, String> record : records) {
                            if (record.key() == null || record.value() == null) {
                                continue;
                            }
                            Alarm alarm = parseAlarm(toAlarmFields(record.key(), record.value()));
                            if (alarm != null) {
                                batch.add(alarm);
                            }
                        }

                        if (!batch.isEmpty()) {
                            Platform.runLater(() -> {
                                if (!running) {
                                    return;
                                }
                                for (Alarm a : batch) {
                                    addAlarm(a);
                                }
                            });
                        }
                    }
                } catch (WakeupException e) {
                    if (!running) {
                        return;
                    }
                } catch (Exception e) {
                    if (running) {
                        LOGGER.info("Kafka consumer error ({}/{}): {}. Trying again...", servers, topicName, e.getMessage());
                        sleepQuietly(2000);
                    }
                } finally {
                    consumer = null;
                }
            }
        }

        private void sleepQuietly(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }
    }

   /**
    * Translation of Phoebus's message
    * @param key PV Route According to the Phoebus Alarm Hierarchy
    * @param json Phoebus Alarm Message
    * @return Map containing the parsed alarm fields, or an empty map if the message is invalid or filtered out
    */
    private Map<String, String> toAlarmFields(String key, String json) {
        Map<String, String> f = new HashMap<>();
        if (key == null || key.isBlank() || json == null || json.isBlank()) {
            return f;
        }

        String path = key.replace("\\/", "/").trim();
        if (path.startsWith("state:")) {
            path = path.substring("state:".length());
        }

        
        if (!PV_SCHEME_PATTERN.matcher(path).find()) {
            return f;
        }

        try {
            JsonNode node = MAPPER.readTree(json);

            if (node.path("severity").asText("").isEmpty()) {
                return f;
            }

            String[] parts = splitPath(path);
            if (parts[1].isEmpty() || !PV_SCHEME_PATTERN.matcher(parts[1]).matches()) {
                return f;
            }

            String formattedLocation = formatLocation(parts[0]);
            if (!matchesLocationFilter(formattedLocation)) {
                return f;
            }

            // Preserve the original full path for “acknowledge” commands
            f.put("full_path", path);
            f.put("location", formattedLocation);
            f.put("pv", parts[1]);

            Instant instant = Instant.now();
            JsonNode t = node.path("time");
            if (t.isObject() && t.has("seconds")) {
                instant = Instant.ofEpochSecond(t.path("seconds").asLong(), t.path("nano").asLong());
            } else if (t.isTextual() && !t.asText().isBlank()) {
                try {
                    instant = Instant.parse(t.asText().trim());
                } catch (Exception ignored) {
                }
            }
            f.put("datetime", DATETIME_FMT.format(instant));

            f.put("severity", node.path("severity").asText(""));
            f.put("message", node.path("message").asText(""));
            f.put("value", formatValue(node.path("value").asText("")));
            f.put("current_severity", node.path("current_severity").asText(""));
            f.put("current_message", node.path("current_message").asText(""));
            f.put("notify", node.path("notify").asText(""));
            f.put("latch", node.path("latch").asText(""));
            f.put("user", node.path("user").asText(""));
            f.put("host", node.path("host").asText(""));
        } catch (Exception e) {
            LOGGER.info("Invalid message (key={}): {} ", key, e.getMessage());
            f.clear();
        }
        return f;
    }

    private static String[] splitPath(String path) {
        int scheme = path.indexOf("://");
        int cut = scheme >= 0 ? path.lastIndexOf('/', scheme) : path.lastIndexOf('/');
        if (cut < 0) {
            return new String[]{"", path};
        }
        return new String[]{path.substring(0, cut), path.substring(cut + 1)};
    }

    /**
     * 
     * @param rawLocation Part taken from the alarm hierarchy path 
     * (e.g., "/Test/factory/p1"), where "Test" is the root alarm configuration name
     * @return Location path without the root configuration prefix (e.g., "factory/p1")
     */
    private static String formatLocation(String rawLocation) {
        if (rawLocation == null || rawLocation.isEmpty()) {
            return "";
        }
        int firstSlash = rawLocation.indexOf('/');
        if (firstSlash < 0) {
            return rawLocation;
        }
        int secondSlash = rawLocation.indexOf('/', firstSlash + 1);
        if (secondSlash < 0) {
            return rawLocation.substring(firstSlash + 1);
        }
        return rawLocation.substring(secondSlash + 1);
    }

    private static String formatValue(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        try {
            return String.format(Locale.ROOT, "%.2f", Double.parseDouble(value));
        } catch (NumberFormatException e) {
            return value;
        }
    }

    private static String baseSeverity(String severity) {
        return severity != null && severity.endsWith("_ACK")
                ? severity.substring(0, severity.length() - "_ACK".length())
                : (severity == null ? "" : severity);
    }

    private static void runOnFx(Runnable r) {
        if (Platform.isFxApplicationThread()) {
            r.run();
        } else {
            Platform.runLater(r);
        }
    }

 
    @Override
    public void addAlarm(Alarm newAlarm) {
        if (newAlarm == null) {
            return;
        }

        if (mode == AlarmMode.RT) {
            int existingIdx = -1;
            for (int i = 0; i < alarmData.size(); i++) {
                if (isSamePv(alarmData.get(i), newAlarm)) {
                    existingIdx = i;
                    break;
                }
            }

            if (existingIdx >= 0) {
                alarmData.remove(existingIdx);
            }
        }

        alarmData.add(0, newAlarm);
        alarmTable.refresh();
        alarmTable.scrollTo(0);

        if (alarmData.size() > MAX_ROWS) {
            alarmData.remove(MAX_ROWS, alarmData.size());
        }
    }

    @Override
    public void clearTable() {
        alarmData.clear();
    }

    @Override
    public void addBufferAlarm(List<Alarm> blockAlarms) {
        if (blockAlarms != null && !blockAlarms.isEmpty()) {
            for (Alarm alarm : blockAlarms) {
                addAlarm(alarm);
            }
        }
    }

    @Override
    public Alarm parseAlarm(Map<String, String> body) {
        if (body == null || body.isEmpty()) {
            return null;
        }
        return new Alarm(body);
    }

    @Override
    public synchronized void shutdown() {
        disposed = true;
        stopSession();
        closeProducer();
    }
}