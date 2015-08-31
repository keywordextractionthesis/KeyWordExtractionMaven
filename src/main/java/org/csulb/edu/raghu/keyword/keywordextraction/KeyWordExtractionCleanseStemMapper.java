package org.csulb.edu.raghu.keyword.keywordextraction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.raghu.keyword.keywordextraction.KeyWordExtractionDriver.CUSTOMCOUNTERS;
import org.csulb.edu.raghu.keyword.util.KeyWordExtractionConstants;
import org.csulb.edu.raghu.keyword.util.Posting;

public class KeyWordExtractionCleanseStemMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	Posting posting;
	Path[] cacheFiles;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(CUSTOMCOUNTERS.TOTAL_POSTINGS).increment(KeyWordExtractionConstants.ONE);
		posting.clear();
		posting.processPosting(value.toString());
		Iterator<Entry<String, Double>> iterator = posting.getTokens().entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String,Double> pair = iterator.next();
			MapWritable tagMapWritable = new MapWritable();

			for (String tag : posting.getTags()) {
				tagMapWritable.put(new Text(tag), new DoubleWritable((double) pair.getValue()));
			}
			context.write(new Text((String) pair.getKey()), tagMapWritable);
			iterator.remove(); // avoids a ConcurrentModificationException
		}

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		posting = new Posting(cacheFiles);
	}

}
