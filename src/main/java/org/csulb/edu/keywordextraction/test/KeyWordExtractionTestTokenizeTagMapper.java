package org.csulb.edu.keywordextraction.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.keywordextraction.util.PostingTagWeight;

public class KeyWordExtractionTestTokenizeTagMapper extends Mapper<Text, MapWritable, Text, PostingTagWeight> {

	LongWritable NEGONE = new LongWritable(-1);
	@Override
	protected void map(Text key, MapWritable value,Context context)
			throws IOException, InterruptedException {
		
		PostingTagWeight ptw = new PostingTagWeight(NEGONE, value);
		context.write(key, ptw);
	}

}
