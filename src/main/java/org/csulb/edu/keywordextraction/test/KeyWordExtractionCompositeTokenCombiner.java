package org.csulb.edu.keywordextraction.test;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.util.PostingTagWeight;

public class KeyWordExtractionCompositeTokenCombiner extends Reducer<Text, PostingTagWeight, Text, PostingTagWeight> {

	@Override
	protected void reduce(Text key, Iterable<PostingTagWeight> values, Context context)
			throws IOException, InterruptedException {
		
		
	}

}
