package org.csulb.edu.keywordextraction.train;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.keywordextraction.regex.RegexFileInputFormat;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

@SuppressWarnings("deprecation")
public class KeyWordExtractionDriver extends Configured implements Tool {

	enum CUSTOMCOUNTERS {
		TOTAL_POSTINGS
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME);

		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(new Path(args[1]), true);

		job.setJarByClass(KeyWordExtractionCleanseStemMapper.class);
		job.setMapperClass(KeyWordExtractionCleanseStemMapper.class);
		job.setReducerClass(KeyWordExtractionCleanseStemReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		job.setInputFormatClass(RegexFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(KeyWordExtractionConstants.FIVE);
		DistributedCache.addCacheFile(new URI(args[2]+"/stop-words_english_1_en.txt"),
				job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[2]+"/stop-words_english_2_en.txt"),
				job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[2]+"/stop-words_english_3_en.txt"),
				job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[2]+"/stop-words_english_4_google_en.txt"),
				job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[2]+"/stop-words_english_5_en.txt"),
				job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[2]+"/stop-words_english_6_en.txt"),
				job.getConfiguration());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyWordExtractionDriver(), args));
	}

}
