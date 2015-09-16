package org.csulb.edu.keywordextraction.tokenscount;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.Posting;

@SuppressWarnings("deprecation")
public class KeyWordExtractionTokenCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	Posting posting;
	Path[] cacheFiles;
	int count = 0;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		posting = new Posting(cacheFiles);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		posting.processPosting(value.toString(), KeyWordExtractionConstants.TRUE);
		Set<String> tokens = posting.getTokens().keySet();
		for (String token : tokens) {
			context.write(new Text(token), KeyWordExtractionConstants.LWONE);
		}
	}

}
