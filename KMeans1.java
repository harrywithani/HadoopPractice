import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

// Regular KMeans clustering using Hadoop 1.x
public class KMeans {

    // Initial centroids (hardcoded for simplicity)
    public static double[][] centroids = {
        {2.0, 2.0}, 
        {8.0, 8.0}
    };

    // ---------------- Mapper ----------------
    public static class KMeansMapper extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value,
                        OutputCollector<IntWritable, Text> output,
                        Reporter reporter) throws IOException {

            StringTokenizer st = new StringTokenizer(value.toString());
            double x = Double.parseDouble(st.nextToken());
            double y = Double.parseDouble(st.nextToken());

            // Find the closest centroid
            int closestCluster = -1;
            double minDistance = Double.MAX_VALUE;

            for (int i = 0; i < centroids.length; i++) {
                double dx = x - centroids[i][0];
                double dy = y - centroids[i][1];
                double distance = dx*dx + dy*dy; // Euclidean distance squared
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCluster = i;
                }
            }

            // Emit clusterId as key, point as value
            output.collect(new IntWritable(closestCluster), new Text(x + "," + y));
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

            while (values.hasNext()) {
                String[] parts = values.next().toString().split(",");
                sumX += Double.parseDouble(parts[0]);
                sumY += Double.parseDouble(parts[1]);
                count++;
            }

            double newX = sumX / count;
            double newY = sumY / count;

            // Output new centroid
            output.collect(key, new Text("(" + newX + "," + newY + ")"));
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

