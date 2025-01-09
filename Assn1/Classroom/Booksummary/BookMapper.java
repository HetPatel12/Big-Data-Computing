

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context ctx)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(",");
		if(parts.length == 5) {
			String subject = parts[3];
			double price = Double.parseDouble(parts[4]);
			ctx.write(new Text(subject), 
						new DoubleWritable(price));
		} else 
			System.out.println("Invalid Record : " + line);
	}
}
