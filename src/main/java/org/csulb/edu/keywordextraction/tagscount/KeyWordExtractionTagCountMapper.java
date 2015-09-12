package org.csulb.edu.keywordextraction.tagscount;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

public class KeyWordExtractionTagCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<String> tags = getTagsFromRecord(value.toString());
		for (String tag : tags) {
			context.write(new Text(tag), KeyWordExtractionConstants.LWONE);
		}
	}

	protected List<String> getTagsFromRecord(String record) {
		String temp[] = record.split(",");
		return Arrays.asList(temp[temp.length-1].split(" "));
	}

}
