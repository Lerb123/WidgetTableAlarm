/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmWidgetRepresentation extends RegionBaseRepresentation<AlarmTableWidget, AlarmWidget> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmWidgetRepresentation.class);
    private final DirtyFlag dirtyKafka = new DirtyFlag();
    private final DirtyFlag dirtyStyle = new DirtyFlag();
    private final DirtyFlag dirtySize = new DirtyFlag();

    @Override
    protected AlarmTableWidget createJFXNode() throws Exception {
        AlarmTableWidget node = new AlarmTableWidget(broker(), topic(), commandTopic());
        node.setLocationFilter(model_widget.propLocationFilter().getValue());
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
        model_widget.propCommandTopicKafka().addUntypedPropertyListener(this::kafkaChanged);

        model_widget.propLocationFilter().addUntypedPropertyListener(this::styleChanged);
        model_widget.propAlarmMode().addUntypedPropertyListener(this::styleChanged);
        model_widget.propHeaderBackgroundColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propHeaderForegroundColor().addUntypedPropertyListener(this::styleChanged);
        model_widget.propHeaderFont().addUntypedPropertyListener(this::styleChanged);
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
            jfx_node.reconfigure(broker(), topic(), commandTopic());
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
        node.setLocationFilter(model_widget.propLocationFilter().getValue());
        node.setAlarmMode(model_widget.propAlarmMode().getValue());

        try {
            node.setHeaderColors(
                    toColor(model_widget.propHeaderBackgroundColor().getValue()),
                    toColor(model_widget.propHeaderForegroundColor().getValue()));
        } catch (Exception e) {
            LOGGER.info("Table Alarm: Error in header colors: {}", e.getMessage());
        }
        try {
            node.setSeverityColors(
                    toColor(model_widget.propMajorAlarmColor().getValue()),
                    toColor(model_widget.propMinorAlarmColor().getValue()),
                    toColor(model_widget.propInvalidAlarmColor().getValue()),
                    toColor(model_widget.propOkAlarmColor().getValue()));
        } catch (Exception e) {
            LOGGER.info("Table Alarm: Error in severity colors: {}", e.getMessage());
        }
        try {
            node.setTableBackground(toColor(model_widget.propBackgroundColor().getValue()));
        } catch (Exception e) {
            LOGGER.info("Table Alarm: Background Color Error: {}", e.getMessage());
        }
        try {
            node.setTableFont(toFont(model_widget.propFont().getValue()));
            node.setHeaderFont(toFont(model_widget.propHeaderFont().getValue()));
        } catch (Exception e) {
            LOGGER.info("Table Alarm: error in the font: {}", e.getMessage());
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

    private String commandTopic() {
        return toolkit.isEditMode() ? "" : model_widget.propCommandTopicKafka().getValue();
    }

    @Override
    public void dispose() {
        jfx_node.shutdown();
        super.dispose();
    }
}
