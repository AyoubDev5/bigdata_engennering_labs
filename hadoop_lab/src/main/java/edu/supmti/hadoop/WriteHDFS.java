package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.nio.charset.StandardCharsets;

public class WriteHDFS {
    public static void main(String[] args) {

        Path filePath = new Path(args[0]);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.19.0.4:9000");

        try (FileSystem fs = FileSystem.get(conf)) {

            if (fs.exists(filePath)) {
                System.out.println("File already exists: " + filePath);
                System.out.println("Overwriting...");
                fs.delete(filePath, true);
            }

            try (FSDataOutputStream outStream = fs.create(filePath)) {
                String message = "Bonjour tout le monde!\n" + args[0] + "\n";
                outStream.write(message.getBytes(StandardCharsets.UTF_8));
                System.out.println("Successfully wrote to: " + filePath);
            }

        } catch (IOException e) {
            System.err.println("I/O error while writing to HDFS:");
            e.printStackTrace();
        }

    }
}
