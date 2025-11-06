package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HadoopFileStatus {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.19.0.2:9000");

        Path originalPath = new Path("/user/root/input","purchases.txt");
        Path newPath = new Path("/user/root/input","achats.txt");

        FileSystem fs = null;
        FileStatus status = null;

        try {
            fs = FileSystem.get(conf);

            if (!fs.exists(originalPath)) {
                System.out.println("Error: File does not exist at " + originalPath.toString());
                return;
            }

            status = fs.getFileStatus(originalPath);

            System.out.println("--- File Status Information ---");
            System.out.println("File Path: " + originalPath);
            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());
            System.out.println("---------------------------------");

            System.out.println("--- Block Locations ---");
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            System.out.println("-------------------------");

            if (fs.rename(originalPath, newPath)) {
                System.out.println("File successfully renamed from " + originalPath.getName()
                        + " to " + newPath.getName());
            } else {
                System.out.println("Failed to rename file.");
            }

        } catch (IOException e) {
            System.err.println("An IOException occurred during HDFS operation:");
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    System.err.println("Error closing FileSystem:");
                    e.printStackTrace();
                }
            }
        }
    }
}
