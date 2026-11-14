/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget;

import java.util.List;
import org.csstudio.display.builder.model.Widget;
import org.csstudio.display.builder.model.WidgetProperty;
import org.csstudio.display.builder.model.WidgetPropertyCategory;
import org.csstudio.display.builder.model.WidgetPropertyDescriptor;
import org.csstudio.display.builder.model.properties.CommonWidgetProperties;

/**
 *
 * @author luis
 */
public class AlarmWidget extends Widget {

    public static final String WIDGET_TYPE = "Table Alarm";

    private static final WidgetPropertyDescriptor<String> propRedisUrl =
            CommonWidgetProperties.newStringPropertyDescriptor(
                    WidgetPropertyCategory.WIDGET, "redis_url", "Redis URL");

    private static final WidgetPropertyDescriptor<String> propRedisChannel =
            CommonWidgetProperties.newStringPropertyDescriptor(
                    WidgetPropertyCategory.WIDGET, "redis_channel", "Redis Channel");

    private WidgetProperty<String> redisUrl;
    private WidgetProperty<String> redisChannel;

    public AlarmWidget() {
        super(WIDGET_TYPE);
    }

    @Override
    protected void defineProperties(List<WidgetProperty<?>> properties) {
        super.defineProperties(properties);

        redisUrl = propRedisUrl.createProperty(this, "");
        redisChannel = propRedisChannel.createProperty(this, "");

        properties.add(redisUrl);
        properties.add(redisChannel);
    }

    public WidgetProperty<String> propRedisUrl() {
        return redisUrl;
    }

    public WidgetProperty<String> propRedisChannel() {
        return redisChannel;
    }
}