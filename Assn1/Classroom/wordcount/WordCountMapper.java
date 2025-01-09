

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text wordWritable = new Text();
	private IntWritable oneWritable = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context ctx)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split("\\W+");
		for (String word : parts) {
			wordWritable.set(word.toLowerCase());
			ctx.write(wordWritable, oneWritable);
		}
	}
}
