
package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget;

import org.csstudio.display.builder.model.Widget;
import org.csstudio.display.builder.model.WidgetCategory;
import org.csstudio.display.builder.model.WidgetDescriptor;

/**
 *
 * @author luis
 */
public class AlarmWidgetDescriptor extends WidgetDescriptor {

    public AlarmWidgetDescriptor() {
        super(AlarmWidget.WIDGET_TYPE, WidgetCategory.MONITOR, "TableAlarm", "/icons/d.png", "Table Alarms");
    }

    @Override
    public Widget createWidget() {
        return new AlarmWidget();
    }
    
}
