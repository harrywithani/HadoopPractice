import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;

public class HDFSFileOperations {

    public static void main(String[] args) {
        try {
            // 1. Configuration and Filesystem setup
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");  // or your NameNode address
            FileSystem fs = FileSystem.get(conf);

            // HDFS path
            Path dirPath = new Path("/user/hadoop/hdfs_demo/");
            Path filePath = new Path("/user/hadoop/hdfs_demo/example.txt");

            // 2. Create Directory
            if (fs.mkdirs(dirPath)) {
                System.out.println("Directory created: " + dirPath.toString());
            } else {
                System.out.println("Directory already exists: " + dirPath.toString());
            }

            // 3. Write to File
            FSDataOutputStream outputStream = fs.create(filePath, true);
            outputStream.writeUTF("Hello from Hadoop HDFS!\nThis is a demo file.\n");
            outputStream.close();
            System.out.println("File written to HDFS: " + filePath.toString());

            // 4. Read File
            System.out.println("\nReading file contents:");
            FSDataInputStream inputStream = fs.open(filePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();

            // 5. Append to File (if append supported)
            if (fs.isFile(filePath)) {
                System.out.println("\nAppending to file...");
                FSDataOutputStream appendStream = fs.append(filePath);
                appendStream.writeUTF("Appended text to this file.\n");
                appendStream.close();
                System.out.println("Append completed.");
            }

            // 6. List files in the directory
            System.out.println("\nListing files in directory:");
            FileStatus[] fileStatuses = fs.listStatus(dirPath);
            for (FileStatus status : fileStatuses) {
                System.out.println("  - " + status.getPath().toString());
            }

            // 7. Read again after append
            System.out.println("\nFile contents after append:");
            inputStream = fs.open(filePath);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();

            // 8. Delete file
            if (fs.delete(filePath, false)) {
                System.out.println("\nFile deleted: " + filePath.toString());
            } else {
                System.out.println("\nFailed to delete file.");
            }

            // 9. Delete directory
            if (fs.delete(dirPath, true)) {
                System.out.println("Directory deleted: " + dirPath.toString());
            }

            fs.close();
            System.out.println("\nAll HDFS operations completed successfully.");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
