/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.apache.plc4x.merlot.alarm.isa18_2.model;

import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;

/**
 *
 * @author luis
 */
public class Alarm {

    private final SimpleStringProperty date;
    private final SimpleStringProperty time;
    private final SimpleIntegerProperty priority;
    private final SimpleStringProperty source;
    private final SimpleStringProperty event;

    public Alarm(String date, String time, int priority, String source, String event) {
        this.date = new SimpleStringProperty(date);
        this.time = new SimpleStringProperty(time);
        this.priority = new SimpleIntegerProperty(priority);
        this.source = new SimpleStringProperty(source);
        this.event = new SimpleStringProperty(event);
    }

    public SimpleStringProperty getDate() {
        return date;
    }

    public SimpleStringProperty getTime() {
        return time;
    }

    public SimpleIntegerProperty getPriority() {
        return priority;
    }

    public SimpleStringProperty getSource() {
        return source;
    }

    public SimpleStringProperty getEvent() {
        return event;
    }
}
