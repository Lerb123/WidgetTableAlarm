
package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget_service;

import java.util.Collection;
import java.util.List;
import org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget.AlarmWidgetDescriptor;
import org.csstudio.display.builder.model.WidgetDescriptor;
import org.csstudio.display.builder.model.spi.WidgetsService;

/**
 *
 * @author luis
 */
public class AlarmWidgetService implements WidgetsService{

    @Override
    public Collection<WidgetDescriptor> getWidgetDescriptors() {
        return List.of(new AlarmWidgetDescriptor());
    }
    
}
