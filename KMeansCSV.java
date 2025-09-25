import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class KMeansCSVWithCentroid {

    // Centroids stored in memory
    public static List<double[]> centroids = new ArrayList<double[]>();

    // --- Mapper ---
    public static class KMeansMapper extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output,
                        Reporter reporter) throws IOException {
            String line = value.toString().trim();
            if (line.startsWith("ID") || line.isEmpty()) return; // skip header

            String[] parts = line.split(",");
            String id = parts[0];
            double math = Double.parseDouble(parts[1]);
            double science = Double.parseDouble(parts[2]);
            double english = Double.parseDouble(parts[3]);

            double[] point = new double[]{math, science, english};

            // Find nearest centroid
            int nearest = 0;
            double minDist = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double[] c = centroids.get(i);
                double dist = 0;
                for (int j = 0; j < c.length; j++) {
                    dist += Math.pow(point[j] - c[j], 2);
                }
                if (dist < minDist) {
                    minDist = dist;
                    nearest = i;
                }
            }

            // Emit: clusterID, "ID,math,science,english"
            output.collect(new IntWritable(nearest), new Text(id + "," + math + "," + science + "," + english));
        }
    }

    // --- Reducer ---
    public static class KMeansReducer extends MapReduceBase
            implements Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterator<Text> values,
                           OutputCollector<IntWritable, Text> output,
                           Reporter reporter) throws IOException {
            List<String> memberIds = new ArrayList<String>();
            double sumMath = 0, sumScience = 0, sumEnglish = 0;
            int count = 0;

            while (values.hasNext()) {
                String[] parts = values.next().toString().split(",");
                String id = parts[0];
                double math = Double.parseDouble(parts[1]);
                double science = Double.parseDouble(parts[2]);
                double english = Double.parseDouble(parts[3]);

                memberIds.add(id);
                sumMath += math;
                sumScience += science;
                sumEnglish += english;
                count++;
            }

            double newMath = sumMath / count;
            double newScience = sumScience / count;
            double newEnglish = sumEnglish / count;

            // Update centroid in memory
            centroids.set(key.get(), new double[]{newMath, newScience, newEnglish});

            // Output format: Centroid=(...), Members=[...]
            String centroidStr = String.format("(%.2f,%.2f,%.2f)", newMath, newScience, newEnglish);
            output.collect(key, new Text("Centroid=" + centroidStr + " Members=" + memberIds.toString()));
        }
    }

    // --- Driver ---
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(KMeansCSVWithCentroid.class);
        conf.setJobName("KMeansCSVWithCentroid");

        conf.setMapperClass(KMeansMapper.class);
        conf.setReducerClass(KMeansReducer.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Initialize centroids manually (example K=2)
        centroids.add(new double[]{30.0, 25.0, 50.0}); // low performers
        centroids.add(new double[]{80.0, 85.0, 79.0}); // high performers

        JobClient.runJob(conf);
    }
}

