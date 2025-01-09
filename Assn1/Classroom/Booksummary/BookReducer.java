import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BookReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context ctx) throws IOException, InterruptedException {
		double total = 0.0;
		for (DoubleWritable price : values) 
			total = total + price.get();
		ctx.write(key, new DoubleWritable(total));
	}
}
