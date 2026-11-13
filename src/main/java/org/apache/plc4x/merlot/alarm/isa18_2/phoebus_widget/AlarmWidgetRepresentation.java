
package org.apache.plc4x.merlot.alarm.isa18_2.phoebus_widget;

import org.apache.plc4x.merlot.alarm.isa18_2.model.AlarmTableWidget;
import org.csstudio.display.builder.representation.javafx.widgets.JFXBaseRepresentation;

/**
 *
 * @author luis
 */
public class AlarmWidgetRepresentation extends JFXBaseRepresentation<AlarmTableWidget, AlarmWidget> {

    @Override
    protected AlarmTableWidget createJFXNode() throws Exception {

        String redisUrl = model_widget.propRedisUrl().getValue();
        String redisChannel = model_widget.propRedisChannel().getValue();

        if (redisUrl == null || redisUrl.isBlank()) {
            throw new IllegalArgumentException("Redis URL is required");
        }
        if (redisChannel == null || redisChannel.isBlank()) {
            throw new IllegalArgumentException("Redis Channel is required");
        }

        return new AlarmTableWidget(redisUrl, redisChannel);
    }

    @Override
    public void dispose() {
        if (jfx_node != null) {
            jfx_node.shutdown();
        }
        super.dispose();
    }

}
