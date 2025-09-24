import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileOperations {

    public static void main(String[] args) throws IOException {
        // HDFS URI (adjust to your cluster)
        String hdfsUri = "hdfs://localhost:9000";

        // Hadoop configuration
        Configuration conf = new Configuration();

        // Get the HDFS filesystem object
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        // ---------------- 1. Create Directory ----------------
        Path dirPath = new Path("/user/testdir");
        if (fs.mkdirs(dirPath)) {
            System.out.println("Directory created: " + dirPath);
        }

        // ---------------- 2. Upload Local File to HDFS ----------------
        Path localFilePath = new Path("/home/user/localfile.txt");
        Path hdfsFilePath = new Path("/user/testdir/hdfsfile.txt");
        fs.copyFromLocalFile(localFilePath, hdfsFilePath);
        System.out.println("File uploaded to HDFS: " + hdfsFilePath);

        // ---------------- 3. Download HDFS File to Local ----------------
        Path localDownloadPath = new Path("/home/user/downloaded.txt");
        fs.copyToLocalFile(hdfsFilePath, localDownloadPath);
        System.out.println("File downloaded from HDFS to local: " + localDownloadPath);

        // ---------------- 4. Read Contents of an HDFS File ----------------
        FSDataInputStream inputStream = fs.open(hdfsFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        System.out.println("Contents of HDFS file:");
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        reader.close();
        inputStream.close();

        // ---------------- 5. Delete a File or Directory ----------------
        if (fs.delete(hdfsFilePath, false)) { // false = not recursive
            System.out.println("File deleted: " + hdfsFilePath);
        }

        // ---------------- 6. Rename a File or Directory ----------------
        Path newPath = new Path("/user/testdir/renamedfile.txt");
        if (fs.rename(localFilePath, newPath)) {
            System.out.println("File renamed to: " + newPath);
        }

        // ---------------- 7. List Files and Directories ----------------
        FileStatus[] fileList = fs.listStatus(dirPath);
        System.out.println("Listing files in " + dirPath + ":");
        for (FileStatus status : fileList) {
            System.out.println(status.getPath());
        }

        // ---------------- 8. Check if File or Directory Exists ----------------
        Path checkPath = new Path("/user/testdir");
        if (fs.exists(checkPath)) {
            System.out.println("Path exists: " + checkPath);
        } else {
            System.out.println("Path does not exist: " + checkPath);
        }

        // ---------------- 9. Check if Path is File or Directory ----------------
        if (fs.isFile(checkPath)) {
            System.out.println(checkPath + " is a file.");
        } else if (fs.isDirectory(checkPath)) {
            System.out.println(checkPath + " is a directory.");
        }

        // ---------------- 10. Get File Size ----------------
        FileStatus fileStatus = fs.getFileStatus(newPath);
        System.out.println("Size of " + newPath + ": " + fileStatus.getLen() + " bytes");

        // Close the filesystem
        fs.close();
    }
}

