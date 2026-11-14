package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget;

import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontPosture;
import javafx.scene.text.FontWeight;
import org.apache.plc4x.merlot.alarm.isa18_2.model.AlarmTableWidget;
import org.csstudio.display.builder.model.DirtyFlag;
import org.csstudio.display.builder.model.WidgetProperty;
import org.csstudio.display.builder.model.properties.WidgetFont;
import org.csstudio.display.builder.representation.javafx.widgets.RegionBaseRepresentation;
import org.phoebus.ui.color.WidgetColor;

public class AlarmWidgetRepresentation extends RegionBaseRepresentation<AlarmTableWidget, AlarmWidget> {

    private final DirtyFlag dirtyKafka = new DirtyFlag();
    private final DirtyFlag dirtyStyle = new DirtyFlag();
    private final DirtyFlag dirtySize = new DirtyFlag();

    @Override
    protected AlarmTableWidget createJFXNode() throws Exception {
        AlarmTableWidget node = new AlarmTableWidget(broker(), topic());
        node.setAlarmMode(model_widget.propAlarmMode().getValue());
        applySize(node);
        applyStyle(node);
        return node;
    }

    @Override
    protected void registerListeners() {
        super.registerListeners();

        model_widget.propWidth().addUntypedPropertyListener(this::sizeChanged);
        model_widget.propHeight().addUntypedPropertyListener(this::sizeChanged);

        model_widget.propBrokerKafka().addUntypedPropertyListener(this::kafkaChanged);
        model_widget.propTopicKafka().addUntypedPropertyListener(this::kafkaChanged);

        model_widget.propAlarmMode().addUntypedPropertyListener(this::styleChanged);
        model_widget.propHeaderBackgroundColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propHeaderForegroundColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propHeaderFontSize().addUntypedPropertyListener(this::styleChanged);
        model_widget.propMajorAlarmColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propMinorAlarmColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propInvalidAlarmColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propOkAlarmColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propBackgroundColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propFont().addUntypedPropertyListener(this::styleChanged);
        model_widget.propEnabled().addUntypedPropertyListener(this::styleChanged);
    }

    private void sizeChanged(final WidgetProperty<?> property, final Object oldValue, final Object newValue) {
        dirtySize.mark();
        toolkit.scheduleUpdate(this);
    }

    private void kafkaChanged(final WidgetProperty<?> property, final Object oldValue, final Object newValue) {
        dirtyKafka.mark();
        toolkit.scheduleUpdate(this);
    }

    private void styleChanged(final WidgetProperty<?> property, final Object oldValue, final Object newValue) {
        dirtyStyle.mark();
        toolkit.scheduleUpdate(this);
    }

    @Override
    public void updateChanges() {
        super.updateChanges();

        if (dirtySize.checkAndClear()) {
            applySize(jfx_node);
        }
        if (dirtyKafka.checkAndClear()) {
            jfx_node.reconfigure(broker(), topic());
        }
        if (dirtyStyle.checkAndClear()) {
            applyStyle(jfx_node);
        }
    }

    private void applySize(AlarmTableWidget node) {
        double w = model_widget.propWidth().getValue();
        double h = model_widget.propHeight().getValue();
        if (w > 0 && h > 0) {
            node.setPrefSize(w, h);
            node.resize(w, h);
        }
    }

    private void applyStyle(AlarmTableWidget node) {
        node.setAlarmMode(model_widget.propAlarmMode().getValue());

        try {
            node.setHeaderColors(
                    toColor(model_widget.propHeaderBackgroundColor().getValue()),
                    toColor(model_widget.propHeaderForegroundColor().getValue()));
        } catch (Exception e) {
            System.err.println("Table Alarm: error en colores de cabecera: " + e);
        }
        try {
            node.setSeverityColors(
                    toColor(model_widget.propMajorAlarmColor().getValue()),
                    toColor(model_widget.propMinorAlarmColor().getValue()),
                    toColor(model_widget.propInvalidAlarmColor().getValue()),
                    toColor(model_widget.propOkAlarmColor().getValue()));
        } catch (Exception e) {
            System.err.println("Table Alarm: error en colores de severidad: " + e);
        }
        try {
            node.setTableBackground(toColor(model_widget.propBackgroundColor().getValue()));
        } catch (Exception e) {
            System.err.println("Table Alarm: error en color de fondo: " + e);
        }
        try {
            WidgetFont baseFont = model_widget.propFont().getValue();
            node.setTableFont(toFont(baseFont));
            int headerSize = model_widget.propHeaderFontSize().getValue();
            node.setHeaderFont(Font.font(baseFont.getFamily(), headerSize));
        } catch (Exception e) {
            System.err.println("Table Alarm: error en la fuente: " + e);
        }
        node.setDisable(!model_widget.propEnabled().getValue());
    }

    private static Color toColor(WidgetColor c) {
        return Color.rgb(c.getRed(), c.getGreen(), c.getBlue(), c.getAlpha() / 255.0);
    }

    private static Font toFont(WidgetFont f) {
        String style = f.getStyle().name();
        return Font.font(
                f.getFamily(),
                style.contains("BOLD") ? FontWeight.BOLD : FontWeight.NORMAL,
                style.contains("ITALIC") ? FontPosture.ITALIC : FontPosture.REGULAR,
                f.getSize());
    }

    private String broker() {
        return toolkit.isEditMode() ? "" : model_widget.propBrokerKafka().getValue();
    }

    private String topic() {
        return toolkit.isEditMode() ? "" : model_widget.propTopicKafka().getValue();
    }

    @Override
    public void dispose() {
        jfx_node.shutdown();
        super.dispose();
    }
}