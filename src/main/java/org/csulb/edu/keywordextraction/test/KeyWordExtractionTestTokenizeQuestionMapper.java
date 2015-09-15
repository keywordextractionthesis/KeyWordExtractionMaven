package org.csulb.edu.keywordextraction.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.keywordextraction.test.KeyWordExtractionTestDriverPhase1.CUSTOMCOUNTERS;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.Posting;
import org.csulb.edu.keywordextraction.util.PostingTagWeight;

@SuppressWarnings("deprecation")
public class KeyWordExtractionTestTokenizeQuestionMapper extends Mapper<LongWritable,Text,Text,PostingTagWeight> {

	Posting posting;
	Path[] cacheFiles;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PostingTagWeight>.Context context)
			throws IOException, InterruptedException {
		
		context.getCounter(CUSTOMCOUNTERS.TOTAL_TEST_POSTINGS).increment(KeyWordExtractionConstants.ONE);
		posting.clear();
		posting.processPosting(value.toString(), KeyWordExtractionConstants.FALSE);
		LongWritable postingId = new LongWritable(posting.getId());
		Iterator<Entry<String, Double>> iterator = posting.getTokens().entrySet().iterator();
		while(iterator.hasNext()){
			context.write(new Text(iterator.next().getKey()), new PostingTagWeight(postingId, new MapWritable()));
		}

	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, PostingTagWeight>.Context context)
			throws IOException, InterruptedException {
		cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		posting = new Posting(cacheFiles);
	}

}
