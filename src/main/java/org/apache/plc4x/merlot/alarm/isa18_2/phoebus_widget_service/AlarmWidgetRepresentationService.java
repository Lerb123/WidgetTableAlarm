
package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget_service;

import java.util.Map;
import static java.util.Map.entry;
import org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget.AlarmWidgetDescriptor;
import org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget.AlarmWidgetRepresentation;
import org.csstudio.display.builder.model.WidgetDescriptor;
import org.csstudio.display.builder.representation.WidgetRepresentation;
import org.csstudio.display.builder.representation.WidgetRepresentationFactory;
import org.csstudio.display.builder.representation.spi.WidgetRepresentationsService;

/**
 *
 * @author luis
 */
public class AlarmWidgetRepresentationService implements WidgetRepresentationsService {

    @Override
    public <TWP, TW> Map<WidgetDescriptor, WidgetRepresentationFactory<TWP, TW>> getWidgetRepresentationFactories() {
        return Map.ofEntries(
                entry(new AlarmWidgetDescriptor(), () -> (WidgetRepresentation) new AlarmWidgetRepresentation()));
    }

}
