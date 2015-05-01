package org.csulb.edu.raghu.keyword.keywordextraction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.csulb.edu.raghu.keyword.util.KeyWordExtractionConstants;
import org.csulb.edu.regex.RegexFileInputFormat;

public class KeyWordExtractionDriver extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {

		Configuration configuration = this.getConf();
		Job job = new Job(configuration, KeyWordExtractionConstants.JOBNAME);

		job.setMapperClass(KeyWordExtractionCleanseStemMapper.class);
		job.setNumReduceTasks(KeyWordExtractionConstants.ZERO);
		job.setInputFormatClass(RegexFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(KeyWordExtractionConstants.TRUE) ? KeyWordExtractionConstants.ZERO
				: KeyWordExtractionConstants.ONE;

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyWordExtractionDriver(), args));
	}

}
