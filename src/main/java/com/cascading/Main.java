
package com.cascading;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * A Cascading example to remove duplicates in a text file
 */
public class Main {

    public static void main(String[] args) {

        System.out.println("Hdfs Job is started");
        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];

        System.out.println("inputPath::" + inputPath);
        System.out.println("outputPath::" + outputPath);

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        Fields users = new Fields("number");
        //Create the source tap
        Tap inTap = new Hfs(new TextDelimited(users, true, "\t"), inputPath);
        //Create the sink tap
        Tap outTap = new Hfs(new TextDelimited(false, "\t"), outputPath, SinkMode.REPLACE);

        // Pipe to connect Source and Sink Tap
        Pipe numberPipe = new Pipe("number");
        Pipe uniquePipe = new Unique(numberPipe, new Fields("number"));
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect("Unique Job", inTap, outTap, uniquePipe);
        flow.complete();
        System.out.println("Hdfs Job is completed");
    }
}

