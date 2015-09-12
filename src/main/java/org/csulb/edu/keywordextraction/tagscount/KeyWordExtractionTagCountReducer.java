package org.csulb.edu.keywordextraction.tagscount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.tagscount.TagFrequencyDriver.CUSTOMCOUNTERS;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

public class KeyWordExtractionTagCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		context.getCounter(CUSTOMCOUNTERS.TOTAL_TAGS).increment(KeyWordExtractionConstants.ONE);
		Iterator<LongWritable> iterator = values.iterator();
		long sum = 0;
		while(iterator.hasNext()){
			sum = sum+iterator.next().get();
		}
		context.write(key, new LongWritable(sum));
	}

}
