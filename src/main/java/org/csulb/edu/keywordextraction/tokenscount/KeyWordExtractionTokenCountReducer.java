package org.csulb.edu.keywordextraction.tokenscount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.tokenscount.TokenFrequencyDriver.CUSTOMCOUNTERS;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;

public class KeyWordExtractionTokenCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		context.getCounter(CUSTOMCOUNTERS.TOTAL_TOKENS).increment(KeyWordExtractionConstants.ONE);
		Iterator<LongWritable> iterator = values.iterator();
		long sum = 0;
		while(iterator.hasNext()){
			sum = sum+iterator.next().get();
		}
		context.write(key, new LongWritable(sum));
	}

}
