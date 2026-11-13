/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package org.apache.plc4x.merlot.alarm.isa18_2.api;

import java.util.List;
import org.apache.plc4x.merlot.alarm.isa18_2.model.Alarm;

/**
 *
 * @author luis
 */
public interface WidgetBehavior {

    /**
     *
     * @param newAlarm Procedimiento estandar, a medida que aparezca una alarma
     * se va anexando
     */
    void addAlarm(Alarm newAlarm);

    /**
     *
     * @param blockAlarms Cargar una lista de alarmas para ser renderizada en la
     * tabla
     */
    void addBufferAlarm(List<Alarm> blockAlarms);

    /**
     *
     * @param messageFromRedis String obtenido del servidor Redis
     * @return Alarm lista para insertarla en la tabla
     */
    Alarm parseAlarm(String messageFromRedis);

    /**
     * Limpia la tabla, opcion usada cuando inicia el widget
     */
    void clearTable();

    /**
     * Cierra la conexión hacoa Redis
     */
    void shutdown();
}
