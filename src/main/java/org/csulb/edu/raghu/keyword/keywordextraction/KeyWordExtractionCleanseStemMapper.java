package org.csulb.edu.raghu.keyword.keywordextraction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.raghu.keyword.keywordextraction.KeyWordExtractionDriver.CUSTOMCOUNTERS;
import org.csulb.edu.raghu.keyword.util.KeyWordExtractionConstants;
import org.csulb.edu.raghu.keyword.util.Posting;

public class KeyWordExtractionCleanseStemMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	Posting posting;
	Path[] cacheFiles;
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(CUSTOMCOUNTERS.TOTAL_POSTINGS).increment(KeyWordExtractionConstants.ONE);
		posting.clear();
		posting.processPosting(value.toString());
		printMap(posting.getTokens());
		
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		cacheFiles =  DistributedCache.getLocalCacheFiles(context
                .getConfiguration()); 
		posting = new Posting(cacheFiles);
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
