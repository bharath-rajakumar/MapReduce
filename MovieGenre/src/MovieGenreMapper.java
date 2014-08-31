import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieGenreMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String file_line = value.toString();
		String[] first_split = file_line.split("\\::");
		String[] second_split = first_split[2].split("\\|");
		for (String genre : second_split) {
			context.write(new Text(genre), new IntWritable(1));
		}
	}

}
