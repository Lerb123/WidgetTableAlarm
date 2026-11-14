package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget;

import java.util.List;
import org.csstudio.display.builder.model.Widget;
import org.csstudio.display.builder.model.WidgetProperty;
import org.csstudio.display.builder.model.WidgetPropertyCategory;
import org.csstudio.display.builder.model.WidgetPropertyDescriptor;
import org.csstudio.display.builder.model.persist.NamedWidgetFonts;
import org.csstudio.display.builder.model.persist.WidgetFontService;
import org.csstudio.display.builder.model.properties.CommonWidgetProperties;
import org.csstudio.display.builder.model.properties.EnumWidgetProperty;
import org.csstudio.display.builder.model.properties.WidgetFont;
import org.csstudio.display.builder.model.widgets.VisibleWidget;
import org.phoebus.ui.color.WidgetColor;

public class AlarmWidget extends VisibleWidget {

    public static final String WIDGET_TYPE = "Table Alarm";

    public enum AlarmMode {
        RT("RT"),
        HTC("HTC");

        private final String label;

        AlarmMode(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    // --- Kafka y Modo ---
    private static final WidgetPropertyDescriptor<String> propBrokerKafka =
            CommonWidgetProperties.newStringPropertyDescriptor(
                    WidgetPropertyCategory.WIDGET, "broker_kafka", "Kafka Broker");

    private static final WidgetPropertyDescriptor<String> propTopicKafka =
            CommonWidgetProperties.newStringPropertyDescriptor(
                    WidgetPropertyCategory.WIDGET, "topic_kafka", "Kafka Topic");

    // Selector RT / HTC
    private static final WidgetPropertyDescriptor<AlarmMode> propAlarmMode =
            new WidgetPropertyDescriptor<>(
                    WidgetPropertyCategory.WIDGET, "alarm_mode", "Mode") {
                @Override
                public WidgetProperty<AlarmMode> createProperty(Widget widget, AlarmMode defaultValue) {
                    return new EnumWidgetProperty<>(this, widget, defaultValue);
                }
            };

    // --- Cabecera (nombres de las columnas) ---
    private static final WidgetPropertyDescriptor<WidgetColor> propHeaderBackgroundColor =
            CommonWidgetProperties.newColorPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "header_background_color", "Header Background Color");

    private static final WidgetPropertyDescriptor<WidgetColor> propHeaderForegroundColor =
            CommonWidgetProperties.newColorPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "header_foreground_color", "Header Foreground Color");

    private static final WidgetPropertyDescriptor<Integer> propHeaderFontSize =
            CommonWidgetProperties.newIntegerPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "header_font_size", "Header Font Size", 6, 72);

    // --- Colores por nivel de severidad ---
    private static final WidgetPropertyDescriptor<WidgetColor> propMajorAlarmColor =
            CommonWidgetProperties.newColorPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "major_alarm_color", "Major Alarm Color");

    private static final WidgetPropertyDescriptor<WidgetColor> propMinorAlarmColor =
            CommonWidgetProperties.newColorPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "minor_alarm_color", "Minor Alarm Color");

    private static final WidgetPropertyDescriptor<WidgetColor> propInvalidAlarmColor =
            CommonWidgetProperties.newColorPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "invalid_alarm_color", "Invalid/Undefined Color");

    private static final WidgetPropertyDescriptor<WidgetColor> propOkAlarmColor =
            CommonWidgetProperties.newColorPropertyDescriptor(
                    WidgetPropertyCategory.DISPLAY, "ok_alarm_color", "OK / Normal Color");

    private WidgetProperty<String> brokerKafka;
    private WidgetProperty<String> topicKafka;
    private WidgetProperty<AlarmMode> alarmMode;
    private WidgetProperty<WidgetColor> headerBackgroundColor;
    private WidgetProperty<WidgetColor> headerForegroundColor;
    private WidgetProperty<Integer> headerFontSize;
    private WidgetProperty<WidgetColor> majorAlarmColor;
    private WidgetProperty<WidgetColor> minorAlarmColor;
    private WidgetProperty<WidgetColor> invalidAlarmColor;
    private WidgetProperty<WidgetColor> okAlarmColor;
    private WidgetProperty<WidgetColor> backgroundColor;
    private WidgetProperty<WidgetFont> font;
    private WidgetProperty<Boolean> enabled;

    public AlarmWidget() {
        super(WIDGET_TYPE, 50, 100);
    }

    @Override
    protected void defineProperties(List<WidgetProperty<?>> properties) {
        super.defineProperties(properties);

        brokerKafka = propBrokerKafka.createProperty(this, "");
        topicKafka = propTopicKafka.createProperty(this, "");
        alarmMode = propAlarmMode.createProperty(this, AlarmMode.RT);

        headerBackgroundColor = propHeaderBackgroundColor.createProperty(this, new WidgetColor(225, 225, 225));
        headerForegroundColor = propHeaderForegroundColor.createProperty(this, new WidgetColor(0, 0, 0));
        headerFontSize = propHeaderFontSize.createProperty(this, 12);

        majorAlarmColor = propMajorAlarmColor.createProperty(this, new WidgetColor(255, 0, 0));
        minorAlarmColor = propMinorAlarmColor.createProperty(this, new WidgetColor(255, 255, 0));
        invalidAlarmColor = propInvalidAlarmColor.createProperty(this, new WidgetColor(51, 51, 51));
        okAlarmColor = propOkAlarmColor.createProperty(this, new WidgetColor(144, 238, 144));

        backgroundColor = CommonWidgetProperties.propBackgroundColor.createProperty(this, new WidgetColor(255, 255, 255));
        font = CommonWidgetProperties.propFont.createProperty(this, WidgetFontService.get(NamedWidgetFonts.DEFAULT));

        enabled = CommonWidgetProperties.propEnabled.createProperty(this, true);

        properties.add(brokerKafka);
        properties.add(topicKafka);
        properties.add(alarmMode);
        properties.add(headerBackgroundColor);
        properties.add(headerForegroundColor);
        properties.add(headerFontSize);
        properties.add(majorAlarmColor);
        properties.add(minorAlarmColor);
        properties.add(invalidAlarmColor);
        properties.add(okAlarmColor);
        properties.add(backgroundColor);
        properties.add(font);
        properties.add(enabled);
    }

    public WidgetProperty<String> propBrokerKafka() { return brokerKafka; }
    public WidgetProperty<String> propTopicKafka() { return topicKafka; }
    public WidgetProperty<AlarmMode> propAlarmMode() { return alarmMode; }
    public WidgetProperty<WidgetColor> propHeaderBackgroundColor() { return headerBackgroundColor; }
    public WidgetProperty<WidgetColor> propHeaderForegroundColor() { return headerForegroundColor; }
    public WidgetProperty<Integer> propHeaderFontSize() { return headerFontSize; }
    public WidgetProperty<WidgetColor> propMajorAlarmColor() { return majorAlarmColor; }
    public WidgetProperty<WidgetColor> propMinorAlarmColor() { return minorAlarmColor; }
    public WidgetProperty<WidgetColor> propInvalidAlarmColor() { return invalidAlarmColor; }
    public WidgetProperty<WidgetColor> propOkAlarmColor() { return okAlarmColor; }
    public WidgetProperty<WidgetColor> propBackgroundColor() { return backgroundColor; }
    public WidgetProperty<WidgetFont> propFont() { return font; }
    public WidgetProperty<Boolean> propEnabled() { return enabled; }
}