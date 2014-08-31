package Main;
import Top10.MovieRatingPair;
import Top10.SortingMapper;
import Top10.SortingReducer;
import TopAverage.MovieRatingMapper;
import TopAverage.MovieRatingReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Top10RatedMovie {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration config_one = new Configuration();
		Configuration config_two = new Configuration();
		
		Job job_one = new Job(config_one,"Movie-Average-Finder");
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(IntWritable.class);
		job_one.setJarByClass(Top10RatedMovie.class);
		job_one.setMapperClass(MovieRatingMapper.class);
		job_one.setReducerClass(MovieRatingReducer.class);
		
		job_one.setInputFormatClass(TextInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		ControlledJob cjob_one = new ControlledJob(config_one);
		cjob_one.setJob(job_one);
		FileInputFormat.setInputPaths(job_one, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_one, new Path("/user/bxr121630/temp-output"));
		
		Job job_two = new Job(config_two,"Movie-Top10-Finder");
		job_two.setOutputKeyClass(NullWritable.class);
		job_two.setOutputValueClass(MovieRatingPair.class);
		job_two.setJarByClass(Top10RatedMovie.class);
		job_two.setMapperClass(SortingMapper.class);
		job_two.setReducerClass(SortingReducer.class);
		
		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		ControlledJob cjob_two = new ControlledJob(config_two);
		cjob_two.setJob(job_two);
		FileInputFormat.setInputPaths(job_two, new Path("/user/bxr121630/temp-output/part-*"));
		FileOutputFormat.setOutputPath(job_two, new Path(args[1]));
		
		JobControl jc_one = new JobControl("Job-Control-One");
		jc_one.addJob(cjob_one);
		jc_one.addJob(cjob_two);
		cjob_two.addDependingJob(cjob_one);
		jc_one.run();
		jc_one.allFinished();
	}

}
