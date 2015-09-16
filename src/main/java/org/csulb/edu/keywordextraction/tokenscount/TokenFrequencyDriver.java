package org.csulb.edu.keywordextraction.tokenscount;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.keywordextraction.regex.RegexFileInputFormat;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

@SuppressWarnings("deprecation")
public class TokenFrequencyDriver extends Configured implements Tool {

	enum CUSTOMCOUNTERS {
		TOTAL_TOKENS
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME_TAGS);

		FileSystem fileSystem = FileSystem.get(configuration);
		fileSystem.delete(new Path(args[1]), true);

		job.setJarByClass(KeyWordExtractionTokenCountMapper.class);
		job.setMapperClass(KeyWordExtractionTokenCountMapper.class);
		job.setCombinerClass(KeyWordExtractionTokenCountReducer.class);
		job.setReducerClass(KeyWordExtractionTokenCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(RegexFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(KeyWordExtractionConstants.THREE);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
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

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TokenFrequencyDriver(), args));
	}

}
