package org.apache.plc4x.merlot.alarm.isa18_2.model;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
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

    private final TableView<Alarm> alarmTable;
    private final ObservableList<Alarm> alarmData;

    //Redis client
//    private final RedisClient redisClient;
//    private final StatefulRedisPubSubConnection<String, String> pubSubConnection;

    public AlarmTableWidget(String hostandPortRedis, String channelRedis) {

        // 1. Inicializar la lista observable
        alarmData = FXCollections.observableArrayList();

        // 2. Configurar la Tabla
        alarmTable = new TableView<>();

        // ¡ESTA ES LA LÍNEA QUE FALTABA!
        alarmTable.setItems(alarmData);

        alarmTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);

        // Configuración de columnas (igual que tu código)
        TableColumn<Alarm, String> colFecha = new TableColumn<>("Date");
        colFecha.setCellValueFactory(new PropertyValueFactory<>("date"));

        TableColumn<Alarm, String> colHora = new TableColumn<>("Time");
        colHora.setCellValueFactory(new PropertyValueFactory<>("time"));

        TableColumn<Alarm, Integer> colPrioridad = new TableColumn<>("Priority");
        colPrioridad.setCellValueFactory(new PropertyValueFactory<>("priority"));

        TableColumn<Alarm, String> colSource = new TableColumn<>("Source");
        colSource.setCellValueFactory(new PropertyValueFactory<>("source"));

        TableColumn<Alarm, String> colEvent = new TableColumn<>("Event");
        colEvent.setCellValueFactory(new PropertyValueFactory<>("event"));

        alarmTable.getColumns().addAll(colFecha, colHora, colPrioridad, colSource, colEvent);

        // 3. Añadir los colores a las filas (RowFactory)
        alarmTable.setRowFactory(tv -> new TableRow<Alarm>() {
            @Override
            protected void updateItem(Alarm item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setStyle("");
                } else {
                    String evento = item.getEvent().toString();
                    // Lógica de colores basada en la imagen
                    if (evento != null && (evento.contains("High Alarm") || evento.contains("HighHigh Alarm") || evento.contains("Overdosed"))) {
                        setStyle("-fx-background-color: #FF0000; -fx-text-background-color: white;"); // Rojo
                    } else if (evento != null && (evento.contains("Low Alarm") || evento.contains("PV: Low Alarm"))) {
                        setStyle("-fx-background-color: #FFFF00; -fx-text-background-color: black;"); // Amarillo
                    } else if (evento != null && evento.contains("Monitoring Fault")) {
                        setStyle("-fx-background-color: #333333; -fx-text-background-color: white;"); // Gris Oscuro
                    } else {
                        setStyle("-fx-background-color: #90EE90; -fx-text-background-color: black;"); // Verde claro
                    }
                }
            }
        });

        // 4. Añadir la tabla al StackPane
        getChildren().add(alarmTable);

//        redisClient = RedisClient.create(hostandPortRedis);
//
//        // 2. Crear una conexión dedicada para Pub/Sub
//        pubSubConnection = redisClient.connectPubSub();
//
//        // 3. Añadir un listener para los mensajes
//        pubSubConnection.addListener(new RedisPubSubAdapter<String, String>() {
//            @Override
//            public void message(String channel, String message) {
//                Platform.runLater(() -> {
//                    Alarm nuevaAlarma = parseAlarm(message);
//                    if (nuevaAlarma != null) {
//                        addAlarm(nuevaAlarma);
//                    }
//                });
//            }
//        });
//
//        // 4. Suscribirse al canal donde se publican las alarmas
//        pubSubConnection.sync().subscribe(channelRedis);
    }

    @Override
    public void addAlarm(Alarm newAlarm) {
        alarmData.add(0, newAlarm);
        alarmTable.scrollTo(0);
        if (alarmData.size() > 1000) {
            alarmData.remove(1000, alarmData.size());
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
        }
    }

    @Override
    public Alarm parseAlarm(String messageFromRedis) {
        //TODO: Usar Jackson para el parseo del json de redis
        return null;
    }

    @Override
    public void shutdown() {
//        if (pubSubConnection != null) {
//            pubSubConnection.close();
//        }
//        if (redisClient != null) {
//            redisClient.shutdown();
//        }
    }
}
