import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//The map reduce job usually split the input data into independent chunks which are processed by the mapping task
//in parallel manner.

//the mapReduce works in two phases: Map and Reduce.
public class endsarsCount{

	//1. Map
	//the line counter mapper class is responsible for splitting and mapping of the data.
	// Execution of map task results into writing output to our local disk, and this is usually
	//to avoid replication which which takes place in the case of HDFS store operation
    public static class LineCntMapper extends Mapper<LongWritable, Text, Text, Int Writable> {
		Text keyEmit = new Text("Total Lines"); 
		private final static IntWritable one = new IntWritable(1);  //initialze a IntWritable class for operation on each record


		//here we perform one map task is created for each splits which executes the map function for
		//each records in the splits. 
		//it waits for the reduce job to p
		public void map(LongWritable key, Text value, Context context){
			try {
				context.write(keyEmit, one);
			}
			//threw an exception for scenarios where the mapping fails to write
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

	//2. Reduce
//this part does the line count for the job
//The LineCntReducer class does the reduce part of the job. After the output of the input has been mapped and sorted
//the LineCntReducer takes it in to perform it's reduction operation.
	public static class LineCntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context){ // we receive the sorted and mapped value from the mapping class
			int sum = 0;
			//here we checked for each sorted mapped value received from the mapper class, we check count how many times this value appears and appends the number to sum
			for (IntWritable val : values) {
				sum += val.get();
			}
			try {
				context.write(key, new IntWritable(sum));
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
    Configuration conf = new Configuration(); // initializing an instance of config
	
	//create an instance of the Job so we can set the mapping and reduce object classes to it
    Job job = Job.getInstance(conf, "line count2");
	
	//here we set the counting class to be the jarByClass. Which is the main class for this map reduce program
    job.setJarByClass(endsarsCount.class);
	
	//we then set out mapper class to be the line count mapping class we created above
    job.setMapperClass(LineCntMapper.class);
	
	//the reducer class we created will be set as the combiner class and the reducer class
    job.setCombinerClass(LineCntReducer.class);
    job.setReducerClass(LineCntReducer.class);
	
	//we set the out put to be a text class
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	
	//when runnignt he jar class, we indicate which path we want to read our input records from
    FileInputFormat.addInputPath(job, new Path(args[0]));
	
	//and then the second argument is where we want to store out final mapReduce output. We indicate the path we want to store it.
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	//if the job we are running completes its operation, it exits the operation.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
