package org.csulb.edu.raghu.keyword.keywordextraction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.raghu.keyword.util.Posting;

public class KeyWordExtractionCleanseStemMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	Posting posting;
	URI[] cacheFiles;
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(CUSTOMCOUNTERS.TOTAL_POSTINGS).increment(KeyWordExtractionConstants.ONE);
		posting = new Posting(value.toString(), cacheFiles);
		printMap(posting.getTokens());
		
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		cacheFiles = context.getCacheFiles(); 
	}
	
	public static void printMap(Map mp) {
	    Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	        it.remove(); // avoids a ConcurrentModificationException
	    }
	}

}
