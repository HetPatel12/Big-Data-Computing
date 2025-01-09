
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BookDriver {
	public static void main(String[] args) {
		try {
			// create & configure job
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "BookSummaryJob");
			job.setJarByClass(BookDriver.class);
			job.setMapperClass(BookMapper.class);
			job.setReducerClass(BookReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			TextInputFormat.addInputPaths(job, "/user/nilesh/books/input/");
			TextOutputFormat.setOutputPath(job, new Path("/user/nilesh/books/output/"));
			// submit job & wait for completion
			job.submit();
			boolean success = job.waitForCompletion(true);
			int ret = success ? 0 : 1;
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
