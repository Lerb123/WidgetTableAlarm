/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package org.apache.plc4x.merlot.alarm.isa18_2;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;
import org.apache.plc4x.merlot.alarm.isa18_2.model.AlarmTableWidget;

/**
 *
 * @author luis
 */



public class MerlotAlarmISA18_2Widget extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        StackPane pane = new AlarmTableWidget("1223", "");
        
        Scene scene = new Scene(pane, 400, 400);
        stage.setTitle("Widget Test");
        stage.setScene(scene);
        stage.show();
    }
    
    
    public static void main(String[] args) {
        launch(args);
    }
    
    
    
}