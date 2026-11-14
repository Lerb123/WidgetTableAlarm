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

public class AlarmTableWidget extends StackPane implements WidgetBehavior {

    private static final int MAX_ROWS = 100;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Valida que la ruta contenga un PV completo con protocolo sim://, pva:// o loc://
    private static final Pattern PV_SCHEME_PATTERN =
            Pattern.compile("(sim|pva|loc)://.+", Pattern.CASE_INSENSITIVE);

    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final TableView<Alarm> alarmTable;
    private final ObservableList<Alarm> alarmData;

    // Parámetros actuales
    private String bootstrapServers;
    private String topic;
    private volatile AlarmMode mode = AlarmMode.RT;

    // Sesión de consumo y productor de comandos Kafka
    private ConsumerSession session;
    private KafkaProducer<String, String> commandProducer;

    private boolean attached = false;
    private boolean disposed = false;

    // Estilo configurable desde las propiedades del widget de Phoebus
    private Color headerBackground;
    private Color headerForeground;
    private Font headerFont;
    private Color tableBackground;
    private Font tableFont;

    // Colores configurables por severidad
    private Color majorColor = Color.web("#FF0000");
    private Color minorColor = Color.web("#FFFF00");
    private Color invalidColor = Color.web("#333333");
    private Color okColor = Color.web("#90EE90");

    public AlarmTableWidget(String bootstrapServers, String topic) {
        // Evitar tamaños predeterminados rígidos en el StackPane
        setMinSize(0, 0);
        setMaxSize(Double.MAX_VALUE, Double.MAX_VALUE);

        // 1. Lista observable
        alarmData = FXCollections.observableArrayList();

        // 2. Tabla sin tamaño fijo: se adapta al contenedor StackPane
        alarmTable = new TableView<>();
        alarmTable.setItems(alarmData);
        alarmTable.setMinSize(0, 0);
        alarmTable.setPrefSize(USE_COMPUTED_SIZE, USE_COMPUTED_SIZE);
        alarmTable.setMaxSize(Double.MAX_VALUE, Double.MAX_VALUE);
        alarmTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);

        // Los nodos de la cabecera solo existen cuando la tabla tiene skin
        alarmTable.skinProperty().addListener((obs, oldSkin, newSkin) -> applyHeaderStyle());

        // Columnas de la tabla (sin Guidance)
        alarmTable.getColumns().add(column("Date/Time", "datetime", 140));
        alarmTable.getColumns().add(column("Severity", "severity", 85));
        alarmTable.getColumns().add(column("Location", "location", 160));
        alarmTable.getColumns().add(column("PV", "pv", 130));
        alarmTable.getColumns().add(column("Message", "message", 180));
        alarmTable.getColumns().add(column("Value", "value", 70));
        alarmTable.getColumns().add(column("User", "user", 80));
        alarmTable.getColumns().add(column("Host", "host", 130));

        // 3. Colores de fila según severidad + menú contextual ("Acknowledge This" / "Acknowledge All")
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

            ContextMenu contextMenu = new ContextMenu();

            MenuItem ackThisItem = new MenuItem("Acknowledge This");
            ackThisItem.setOnAction(evt -> {
                Alarm selected = row.getItem();
                if (selected != null) {
                    acknowledgeAlarm(selected);
                }
            });

            MenuItem ackAllItem = new MenuItem("Acknowledge All");
            ackAllItem.setOnAction(evt -> acknowledgeAllAlarms());

            contextMenu.getItems().addAll(ackThisItem, ackAllItem);

            // Mostrar menú contextual solo cuando la fila contiene una alarma válida
            row.contextMenuProperty().bind(
                    Bindings.when(row.emptyProperty())
                            .then((ContextMenu) null)
                            .otherwise(contextMenu)
            );

            // Habilitar dinámicamente cada opción según el estado actual
            contextMenu.setOnShowing(evt -> {
                Alarm item = row.getItem();
                boolean canAckThis = item != null && canAcknowledge(item.getSeverity());
                boolean canAckAny = alarmData.stream().anyMatch(a -> canAcknowledge(a.getSeverity()));

                ackThisItem.setDisable(!canAckThis);
                ackAllItem.setDisable(!canAckAny);
            });

            return row;
        });

        // 4. Añadir la tabla
        getChildren().add(alarmTable);

        sceneProperty().addListener((obs, oldScene, newScene) -> setAttached(newScene != null));

        reconfigure(bootstrapServers, topic);
    }

    // ------------------------------------------------------------------
    // Acknowledge hacia Phoebus Alarm Server (Kafka Command Topic)
    // ------------------------------------------------------------------

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
     * Reconoce una alarma individual ("Acknowledge This").
     */
    public void acknowledgeAlarm(Alarm alarm) {
        if (alarm == null || !canAcknowledge(alarm.getSeverity())) {
            return;
        }
        acknowledgeBatch(Collections.singletonList(alarm));
    }

    /**
     * Reconoce todas las alarmas activas sin reconocer en la tabla ("Acknowledge All").
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
     * Envía los comandos "acknowledge" al tópico "<topic>Command" de Kafka para una o varias alarmas
     * y actualiza su estado visual a *_ACK.
     */
    private void acknowledgeBatch(List<Alarm> alarmsToAck) {
        if (alarmsToAck == null || alarmsToAck.isEmpty()
                || bootstrapServers == null || bootstrapServers.isEmpty()
                || topic == null || topic.isEmpty()) {
            return;
        }

        final String commandTopic = topic.endsWith("Command") ? topic : topic + "Command";
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
            System.err.println("Error serializando comando acknowledge: " + e.getMessage());
            return;
        }

        List<String> commandKeys = new ArrayList<>();

        for (Alarm alarm : alarmsToAck) {
            if (!canAcknowledge(alarm.getSeverity())) {
                continue;
            }

            String fullPath = alarm.get("full_path");
            if (fullPath == null || fullPath.isEmpty()) {
                String loc = alarm.getLocation();
                String pv = alarm.getPv();
                fullPath = (loc == null || loc.isEmpty()) ? pv : "/" + topic + "/" + loc + "/" + pv;
            }

            String commandKey = "command:" + fullPath;
            if (!commandKeys.contains(commandKey)) {
                commandKeys.add(commandKey);
            }

            // Actualización visual inmediata en la fila
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

        // Publicación asíncrona en Kafka
        Thread sender = new Thread(() -> {
            try {
                KafkaProducer<String, String> producer = getOrCreateProducer();
                if (producer != null) {
                    for (String key : commandKeys) {
                        producer.send(new ProducerRecord<>(commandTopic, key, jsonPayload), (meta, ex) -> {
                            if (ex != null) {
                                System.err.println("Error enviando acknowledge (" + key + ") a Kafka ("
                                        + commandTopic + "): " + ex.getMessage());
                            }
                        });
                    }
                    producer.flush();
                }
            } catch (Exception e) {
                System.err.println("Fallo al publicar acknowledge en Kafka: " + e.getMessage());
            }
        }, "kafka-alarm-ack");
        sender.setDaemon(true);
        sender.start();
    }

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

    private static String resolveHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    // ------------------------------------------------------------------
    // Modo RT / HTC
    // ------------------------------------------------------------------

    public void setAlarmMode(AlarmMode newMode) {
        AlarmMode targetMode = (newMode == null) ? AlarmMode.RT : newMode;
        if (this.mode != targetMode) {
            this.mode = targetMode;
            // Si se cambia a RT en caliente, depurar duplicados dejando el registro más reciente por PV
            if (this.mode == AlarmMode.RT) {
                runOnFx(this::deduplicateForRt);
            }
        }
    }

    public AlarmMode getAlarmMode() {
        return mode;
    }

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
     * Compara únicamente por el nombre del PV.
     */
    private static boolean isSamePv(Alarm a, Alarm b) {
        if (a == null || b == null) {
            return false;
        }
        String pvA = a.getPv() == null ? "" : a.getPv().trim();
        String pvB = b.getPv() == null ? "" : b.getPv().trim();
        return !pvA.isEmpty() && Objects.equals(pvA, pvB);
    }

    // ------------------------------------------------------------------
    // Estilo (lo llama la representación de Phoebus)
    // ------------------------------------------------------------------

    public void setHeaderColors(Color background, Color foreground) {
        headerBackground = background;
        headerForeground = foreground;
        applyHeaderStyle();
    }

    /** Configura los colores de fondo de fila para cada nivel de severidad. */
    public void setSeverityColors(Color major, Color minor, Color invalid, Color ok) {
        if (major != null)   this.majorColor = major;
        if (minor != null)   this.minorColor = minor;
        if (invalid != null) this.invalidColor = invalid;
        if (ok != null)      this.okColor = ok;
        runOnFx(alarmTable::refresh);
    }

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

    private static String buildRowCss(Color bg) {
        Color c = (bg == null) ? Color.WHITE : bg;
        // Luminancia relativa para elegir contraste blanco o negro automáticamente
        double luminance = 0.299 * c.getRed() + 0.587 * c.getGreen() + 0.114 * c.getBlue();
        String textColor = (luminance < 0.5) ? "white" : "black";
        return "-fx-background-color: " + toRgba(c) + "; -fx-text-background-color: " + textColor + ";";
    }

    /** Fuente exclusiva de la cabecera (títulos de columna). */
    public void setHeaderFont(Font font) {
        headerFont = font;
        applyHeaderStyle();
    }

    public void setTableBackground(Color background) {
        tableBackground = background;
        refreshTableStyle();
    }

    public void setTableFont(Font font) {
        tableFont = font;
        refreshTableStyle();
        applyHeaderStyle();
    }

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
            System.err.println("Error aplicando estilo a la tabla [" + css + "]: " + e);
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
                System.err.println("Error aplicando estilo a la cabecera: " + e);
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

    private TableColumn<Alarm, String> column(String title, String key, double width) {
        TableColumn<Alarm, String> col = new TableColumn<>(title);
        col.setCellValueFactory(cd -> new ReadOnlyStringWrapper(cd.getValue().get(key)));
        col.setPrefWidth(width);
        return col;
    }

    // ------------------------------------------------------------------
    // Configuración desde la UI
    // ------------------------------------------------------------------

    public synchronized void reconfigure(String newBootstrapServers, String newTopic) {
        String servers = newBootstrapServers == null ? "" : newBootstrapServers.trim();
        String top = newTopic == null ? "" : newTopic.trim();

        if (servers.equals(bootstrapServers) && top.equals(topic)) {
            return;
        }

        bootstrapServers = servers;
        topic = top;

        closeProducer();
        runOnFx(this::clearTable);
        restartSession();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
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

    // ------------------------------------------------------------------
    // Consumidor Kafka
    // ------------------------------------------------------------------

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
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
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
                        System.err.println("Error en consumidor Kafka (" + servers + "/" + topicName
                                + "): " + e.getMessage() + ". Reintentando...");
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

    // ------------------------------------------------------------------
    // Traducción del mensaje de Phoebus
    // ------------------------------------------------------------------

    private Map<String, String> toAlarmFields(String key, String json) {
        Map<String, String> f = new HashMap<>();
        if (key == null || key.isBlank() || json == null || json.isBlank()) {
            return f;
        }

        String path = key.replace("\\/", "/").trim();
        if (path.startsWith("state:")) {
            path = path.substring("state:".length());
        }

        // Aceptar únicamente keys con la ruta completa de la variable ([sim|pva|loc]://...)
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

            // Conservar la ruta completa original para comandos "acknowledge"
            f.put("full_path", path);
            f.put("location", formatLocation(parts[0]));
            f.put("pv", parts[1]);

            // Extraer fecha/hora actualizada del mensaje (o Instant.now() si no viene)
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
            System.err.println("Mensaje inválido (key=" + key + "): " + e.getMessage());
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
     * Toma únicamente la ruta que está después del segundo '/'.
     * Ejemplo: "/Prueba/factory/p1" -> "factory/p1"
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
        if (value.isEmpty()) {
            return value;
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

    // ------------------------------------------------------------------
    // WidgetBehavior
    // ------------------------------------------------------------------

    @Override
    public void addAlarm(Alarm newAlarm) {
        if (newAlarm == null) {
            return;
        }

        if (mode == AlarmMode.RT) {
            // Si existe un registro con el mismo PV (sea cual sea su severity),
            // se reemplaza con todos los parámetros nuevos (incluyendo fecha y hora)
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

        // Insertar al principio con los parámetros y fecha/hora actualizados
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