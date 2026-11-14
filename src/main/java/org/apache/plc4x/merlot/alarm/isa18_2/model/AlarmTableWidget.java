//pac


package org.apache.plc4x.merlot.alarm.isa18_2.model;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.StackPane;
import org.apache.plc4x.merlot.alarm.isa18_2.api.WidgetBehavior;


public class AlarmTableWidget extends StackPane implements WidgetBehavior {

    private static final int MAX_ROWS = 100;

    private final TableView<Alarm> alarmTable;
    private final ObservableList<Alarm> alarmData;

    // Redis
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private Thread streamThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AlarmTableWidget(String hostandPortRedis, String streamKey) {

        // 1. Lista observable
        alarmData = FXCollections.observableArrayList();

        // 2. Tabla
        alarmTable = new TableView<>();
        alarmTable.setItems(alarmData);
        alarmTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);

        TableColumn<Alarm, String> colFecha = new TableColumn<>("Date");
        colFecha.setCellValueFactory(new PropertyValueFactory<>("date"));

        TableColumn<Alarm, String> colHora = new TableColumn<>("Time");
        colHora.setCellValueFactory(new PropertyValueFactory<>("time"));

        TableColumn<Alarm, Number> colPrioridad = new TableColumn<>("Priority");
        colPrioridad.setCellValueFactory(new PropertyValueFactory<>("priority"));

        TableColumn<Alarm, String> colSource = new TableColumn<>("Source");
        colSource.setCellValueFactory(new PropertyValueFactory<>("source"));

        TableColumn<Alarm, String> colEvent = new TableColumn<>("Event");
        colEvent.setCellValueFactory(new PropertyValueFactory<>("event"));

        alarmTable.getColumns().addAll(colFecha, colHora, colPrioridad, colSource, colEvent);

        // 3. Colores de fila
        alarmTable.setRowFactory(tv -> new TableRow<Alarm>() {
            @Override
            protected void updateItem(Alarm item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setStyle("");
                } else {
                    String evento = item.getEvent() == null ? "" : item.getEvent();
                    if (evento.contains("High Alarm") || evento.contains("HighHigh Alarm") || evento.contains("Overdosed")) {
                        setStyle("-fx-background-color: #FF0000; -fx-text-background-color: white;"); // Rojo
                    } else if (evento.contains("Low Alarm")) { // incluye "PV: Low Alarm"
                        setStyle("-fx-background-color: #FFFF00; -fx-text-background-color: black;"); // Amarillo
                    } else if (evento.contains("Monitoring Fault")) {
                        setStyle("-fx-background-color: #333333; -fx-text-background-color: white;"); // Gris oscuro
                    } else {
                        setStyle("-fx-background-color: #90EE90; -fx-text-background-color: black;"); // Verde claro
                    }
                }
            }
        });

        // 4. Añadir la tabla
        getChildren().add(alarmTable);

        if (hostandPortRedis != null && streamKey != null) {
            startStreamReader(hostandPortRedis, streamKey);
        }
    }

    private void startStreamReader(String uri, String streamKey) {
        try {
            redisClient = RedisClient.create(uri);
            // Conexión DEDICADA: XREAD BLOCK bloquea la conexión
            connection = redisClient.connect();
            running.set(true);

            streamThread = new Thread(() -> readLoop(streamKey), "redis-stream-reader");
            streamThread.setDaemon(true);
            streamThread.start();
        } catch (Exception e) {
            System.err.println("Error conectando a Redis: " + e.getMessage());
        }
    }

    private void readLoop(String streamKey) {
        RedisCommands<String, String> sync = connection.sync();

        // Resolvemos "solo nuevos" a un ID concreto. Si usáramos "$" en cada
        // XREAD, se perderían los mensajes llegados entre dos timeouts.
        // Para leer todo el histórico: String lastId = "0-0";
        String lastId = currentLastId(sync, streamKey);

        // Opcional: cargar los últimos N eventos al arrancar
        // lastId = loadHistory(sync, streamKey, 100);

        XReadArgs args = XReadArgs.Builder.block(Duration.ofSeconds(2)).count(100);

        while (running.get()) {
            try {
                List<StreamMessage<String, String>> messages =
                        sync.xread(args, StreamOffset.from(streamKey, lastId));

                if (messages == null || messages.isEmpty()) {
                    continue; // timeout del BLOCK, reintentar
                }

                List<Alarm> batch = new ArrayList<>();
                for (StreamMessage<String, String> msg : messages) {
                    lastId = msg.getId(); // avanzar el cursor
                    Alarm alarm = parseAlarm(msg.getBody());
                    if (alarm != null) {
                        batch.add(alarm);
                    }
                }

                if (!batch.isEmpty()) {
                    Platform.runLater(() -> {
                        for (Alarm a : batch) {
                            addAlarm(a);
                        }
                    });
                }
            } catch (Exception e) {
                if (!running.get()) {
                    return; // cierre solicitado
                }
                System.err.println("Error leyendo stream: " + e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** ID de la última entrada del stream, o "0-0" si está vacío o no existe. */
    private String currentLastId(RedisCommands<String, String> sync, String streamKey) {
        try {
            List<StreamMessage<String, String>> last =
                    sync.xrevrange(streamKey, Range.unbounded(), Limit.from(1));
            if (last != null && !last.isEmpty()) {
                return last.get(0).getId();
            }
        } catch (Exception e) {
            System.err.println("No se pudo obtener el último ID: " + e.getMessage());
        }
        return "0-0";
    }

    /** Carga las últimas N entradas (de más antigua a más nueva) y devuelve el último ID. */
    private String loadHistory(RedisCommands<String, String> sync, String streamKey, long n) {
        List<StreamMessage<String, String>> last =
                new ArrayList<>(sync.xrevrange(streamKey, Range.unbounded(), Limit.from(n)));
        if (last.isEmpty()) {
            return "0-0";
        }

        // xrevrange devuelve de más nueva a más antigua; addBufferAlarm inserta
        // al principio, así que mantenemos ese orden (la más nueva arriba).
        List<Alarm> alarms = new ArrayList<>();
        for (StreamMessage<String, String> m : last) {
            Alarm a = parseAlarm(m.getBody());
            if (a != null) {
                alarms.add(a);
            }
        }
        Platform.runLater(() -> addBufferAlarm(alarms));
        return last.get(0).getId(); // la más nueva
    }

    @Override
    public void addAlarm(Alarm newAlarm) {
        alarmData.add(0, newAlarm);
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
            alarmData.addAll(0, blockAlarms);
            if (alarmData.size() > MAX_ROWS) {
                alarmData.remove(MAX_ROWS, alarmData.size());
            }
        }
    }

    /**
     * Espera entradas publicadas con campos sueltos, p. ej.:
     * XADD alarms * date 2025-01-01 time 10:00:00 priority 1 source TK-101 event "High Alarm"
     */
    @Override
    public Alarm parseAlarm(Map<String, String> body) {
        try {
            return new Alarm(
                    body.get("date"),
                    body.get("time"),
                    Integer.parseInt(body.getOrDefault("priority", "0")),
                    body.get("source"),
                    body.get("event"));
        } catch (Exception e) {
            System.err.println("Alarma inválida " + body + ": " + e.getMessage());
            return null;
        }
    }

    @Override
    public void shutdown() {
        running.set(false);
        if (connection != null) {
            connection.close(); // desbloquea el XREAD en curso
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
        if (streamThread != null) {
            streamThread.interrupt();
        }
    }
}