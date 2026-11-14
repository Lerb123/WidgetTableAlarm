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

    public SimpleStringProperty dateProperty() {
        return date;
    }

    public SimpleStringProperty timeProperty() {
        return time;
    }

    public SimpleIntegerProperty priorityProperty() {
        return priority;
    }

    public SimpleStringProperty sourceProperty() {
        return source;
    }

    public SimpleStringProperty eventProperty() {
        return event;
    }

    // Getters de valor (convención JavaBean)
    public String getDate() {
        return date.get();
    }

    public String getTime() {
        return time.get();
    }

    public int getPriority() {
        return priority.get();
    }

    public String getSource() {
        return source.get();
    }

    public String getEvent() {
        return event.get();
    }

}
