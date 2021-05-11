import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 public class ReduceJoinData {
 public static class nigeriaMapper extends Mapper <LongWritable, Text, Text, Text>
 {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
        {
                String record = value.toString();
                String[] parts = record.split(",");
                try{
					//foreign join is the user ID
                context.write(new Text(parts[0]), new Text("location   " + parts[1]));
                 }
                catch (IOException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
                catch (InterruptedException e) {
                         e.printStackTrace();
                        System.exit(0);
                }

        }
 }

public static class buhariMapper extends Mapper <LongWritable, Text, Text, Text>
 {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
        {
        String record = value.toString();
        String[] parts = record.split(",");
        try{
			//foreign join is the user ID
        context.write(new Text(parts[2]), new Text("location   " + parts[3]));
         }
         catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
         }
         catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(0);
         }


        }
 }

 public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
        {
         String name = "";
         double total = 0.0;
         int count = 0;
         for (Text t : values)
         {
         String parts[] = t.toString().split("  ");
         if (parts[0].equals("location"))
         {
         count++;
        total += Float.parseFloat(parts[1]);
        }
        else if (parts[0].equals("location"))
        {
        name = parts[1];
        }
        }
        String str = String.format("%d %f", count, total);
        try{
        context.write(new Text(name), new Text(str));
         }
                catch (IOException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
                catch (InterruptedException e) {
                         e.printStackTrace();
                        System.exit(0);
                }

 }
 }

 public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(ReduceJoinData.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, nigeriaMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, buhariMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
 }
