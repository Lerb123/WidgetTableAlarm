package org.apache.plc4x.merlot.alarm.isa18_2.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Alarm {

    private final Map<String, String> fields;

    public Alarm(Map<String, String> fields) {
        this.fields = Collections.unmodifiableMap(new HashMap<>(fields));
    }

    /** Valor del campo, o cadena vacía si no existe. */
    public String get(String key) {
        return fields.getOrDefault(key, "");
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public String getLocation() {
        return get("location");
    }

    public String getPv() {
        return get("pv");
    }

    public String getSeverity() {
        return get("severity");
    }

    public String getMessage() {
        return get("message");
    }
}