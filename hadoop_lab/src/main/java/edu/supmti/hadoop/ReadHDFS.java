package edu.supmti.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class ReadHDFS {

    public static void main(String[] args) throws Exception {

        Path filePath = new Path("/user/root/purchases.txt");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.19.0.4:9000");
        System.out.println("Using filesystem: " + FileSystem.get(conf).getUri());

        try (FileSystem fs = FileSystem.get(conf);
             FSDataInputStream in = fs.open(filePath);
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

            if (!fs.exists(filePath)) {
                System.err.println("Error: File not found at " + filePath);
                return;
            }

            System.out.println("--- Reading " + filePath.getName() + " ---");
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            System.out.println("--- End of File ---");

        } catch (IOException e) {
            e.printStackTrace();
        }
//        Path filePath = new Path("/user/root/purchases.txt");
//
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000/");
//
//        FileSystem fs = null;
//        FSDataInputStream inStream = null;
//        BufferedReader br = null;
//
//        try {
//            fs = FileSystem.get(conf);
//
//            if (!fs.exists(filePath)) {
//                System.err.println("Error: File not found at " + filePath.toString());
//            }
//
//            inStream = fs.open(filePath);
//            InputStreamReader isr = new InputStreamReader(inStream);
//            br = new BufferedReader(isr);
//
//            String line;
//            System.out.println("--- Reading Contents of " + filePath.getName() + " ---");
//
//            while ((line = br.readLine()) != null) {
//                System.out.println(line);
//            }
//
//            System.out.println("--- End of File ---");
//
//        } catch (IOException e) {
//            System.err.println("An I/O error occurred during file reading:");
//            e.printStackTrace();
//        } finally {
//            if (br != null) br.close();
//            else if (inStream != null) inStream.close();
//            if (fs != null) fs.close();
//        }

    }
}
