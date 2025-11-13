import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentKMeans {

    public static class KMapper extends Mapper<Object, Text, IntWritable, Text> {

        private double[][] centroids = { {30.0, 30.0}, {80.0, 85.0} };

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            String student = parts[0];
            double score1 = Double.parseDouble(parts[1]);
            double score2 = Double.parseDouble(parts[2]);

            int nearest = 0;
            double minDist = Double.MAX_VALUE;

            for (int i = 0; i < centroids.length; i++) {
                double dx = score1 - centroids[i][0];
                double dy = score2 - centroids[i][1];
                double dist = dx * dx + dy * dy;
                if (dist < minDist) {
                    minDist = dist;
                    nearest = i;
                }
            }

            context.write(new IntWritable(nearest), new Text(student + "," + score1 + "," + score2));
        }
    }

    public static class KReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sumX = 0, sumY = 0;
            int count = 0;
            StringBuilder students = new StringBuilder();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                students.append(parts[0]).append(" ");
                sumX += Double.parseDouble(parts[1]);
                sumY += Double.parseDouble(parts[2]);
                count++;
            }

            double newX = sumX / count;
            double newY = sumY / count;

            context.write(key, new Text("Centroid=(" + newX + "," + newY + ") Students=" + students.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "student kmeans");
        job.setJarByClass(StudentKMeans.class);
        job.setMapperClass(KMapper.class);
        job.setReducerClass(KReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
