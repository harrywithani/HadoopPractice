import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KMeans {

    // ---------------- Mapper ----------------
    public static class KMeansMapper extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, Text> {

        // Hard-coded centroids for simplicity (update via driver in real multiple iterations)
        public static double[][] centroids = { {2.0, 2.0}, {8.0, 8.0} };

        public void map(Object key, Text value,
                        OutputCollector<IntWritable, Text> output,
                        Reporter reporter) throws IOException {
            // Input format: student_id x y
            StringTokenizer st = new StringTokenizer(value.toString());
            String studentId = st.nextToken();
            double x = Double.parseDouble(st.nextToken());
            double y = Double.parseDouble(st.nextToken());

            // Find the closest centroid
            int closestCluster = -1;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.length; i++) {
                double dist = Math.pow(x - centroids[i][0], 2) + Math.pow(y - centroids[i][1], 2);
                if (dist < minDistance) {
                    minDistance = dist;
                    closestCluster = i;
                }
            }

            // Emit clusterId as key, value = studentId,x,y
            output.collect(new IntWritable(closestCluster), new Text(studentId + "," + x + "," + y));
        }
    }

    // ---------------- Reducer ----------------
    public static class KMeansReducer extends MapReduceBase
            implements Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterator<Text> values,
                           OutputCollector<IntWritable, Text> output,
                           Reporter reporter) throws IOException {
            double sumX = 0, sumY = 0;
            int count = 0;
            List<String> members = new ArrayList<>();

            while (values.hasNext()) {
                String[] parts = values.next().toString().split(",");
                String studentId = parts[0];
                double x = Double.parseDouble(parts[1]);
                double y = Double.parseDouble(parts[2]);

                sumX += x;
                sumY += y;
                count++;

                // Only store student ID in the member list
                members.add(studentId);
            }

            double newX = sumX / count;
            double newY = sumY / count;

            output.collect(key, new Text("Centroid=(" + newX + "," + newY + ") Members=" + members));
        }
    }

    // ---------------- Main ----------------
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(KMeans.class);
        conf.setJobName("kmeans");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(KMeansMapper.class);
        conf.setReducerClass(KMeansReducer.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}

