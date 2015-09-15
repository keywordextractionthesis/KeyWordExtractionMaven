package org.csulb.edu.keywordextraction.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

public class KeyWordExtractionTestDriverPhase2 extends Configured implements Tool {

	enum CUSTOMCOUNTERS {
		TOTAL_TEST_POSTINGS
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyWordExtractionTestDriverPhase2(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		configuration.set("mapreduce.output.textoutputformat.separator", KeyWordExtractionConstants.COMMA);
		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(new Path(args[1]), true);
		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME);
		job.setJarByClass(KeyWordExtractionQuestionCollectMapper.class);
		job.setMapperClass(KeyWordExtractionQuestionCollectMapper.class);
		job.setReducerClass(KeyWordExtractionQuestionCollectionReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;
	}

}
