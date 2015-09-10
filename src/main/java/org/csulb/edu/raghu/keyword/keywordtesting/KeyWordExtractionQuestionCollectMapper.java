package org.csulb.edu.raghu.keyword.keywordtesting;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyWordExtractionQuestionCollectMapper
		extends Mapper<LongWritable, MapWritable, LongWritable, MapWritable> {

	@Override
	protected void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}

}
