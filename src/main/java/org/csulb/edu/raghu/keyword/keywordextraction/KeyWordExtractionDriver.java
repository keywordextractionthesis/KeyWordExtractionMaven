package org.csulb.edu.raghu.keyword.keywordextraction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.raghu.keyword.util.KeyWordExtractionConstants;
import org.csulb.edu.raghu.regex.RegexFileInputFormat;

public class KeyWordExtractionDriver extends Configured implements Tool {
	
	 enum CUSTOMCOUNTERS{
		TOTAL_POSTINGS
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME);

		job.setMapperClass(KeyWordExtractionCleanseStemMapper.class);
		job.setNumReduceTasks(KeyWordExtractionConstants.ZERO);
		job.setInputFormatClass(RegexFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.addCacheFile(new Path("/user/mapper/keywordextract/cache/stop-words_english_1_en.txt").toUri());
		job.addCacheFile(new Path("/user/mapper/keywordextract/cache/stop-words_english_2_en.txt").toUri());
		job.addCacheFile(new Path("/user/mapper/keywordextract/cache/stop-words_english_3_en.txt").toUri());
		job.addCacheFile(new Path("/user/mapper/keywordextract/cache/stop-words_english_4_google_en.txt").toUri());
		job.addCacheFile(new Path("/user/mapper/keywordextract/cache/stop-words_english_5_en.txt").toUri());
		job.addCacheFile(new Path("/user/mapper/keywordextract/cache/stop-words_english_6_en.txt").toUri());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyWordExtractionDriver(), args));
	}

}
