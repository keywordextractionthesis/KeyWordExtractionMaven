package org.csulb.edu.keywordextraction.test;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.keywordextraction.regex.RegexFileInputFormat;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.PostingTagWeight;

@SuppressWarnings("deprecation")
public class KeyWordExtractionTestDriverPhase1 extends Configured implements Tool {

	enum CUSTOMCOUNTERS {
		TOTAL_TEST_POSTINGS
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME);

		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(new Path(args[2]), true);
		MultipleInputs.addInputPath(job, new Path(args[0]), RegexFileInputFormat.class,
				KeyWordExtractionTestTokenizeQuestionMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class,
				KeyWordExtractionTestTokenizeTagMapper.class);
		job.setJarByClass(KeyWordExtractionTestTokenizeQuestionMapper.class);
		job.setJarByClass(KeyWordExtractionTestTokenizeTagMapper.class);
		job.setReducerClass(KeyWordExtractionCompositeTokenReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PostingTagWeight.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(MapWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(3);
		DistributedCache.addCacheFile(new URI(args[3] + "/stop-words_english_1_en.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[3] + "/stop-words_english_2_en.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[3] + "/stop-words_english_3_en.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[3] + "/stop-words_english_4_google_en.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[3] + "/stop-words_english_5_en.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[3] + "/stop-words_english_6_en.txt"), job.getConfiguration());

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyWordExtractionTestDriverPhase1(), args));
	}

}
