package org.csulb.edu.raghu.keyword.keywordtesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.raghu.keyword.util.PostingTagWeight;
import org.csulb.edu.raghu.keyword.util.KeyWordExtractionConstants;

public class KeyWordExtractionCompositeTokenReducer extends Reducer<Text, PostingTagWeight, LongWritable, MapWritable> {

	@Override
	protected void reduce(Text key, Iterable<PostingTagWeight> values, Context context)
			throws IOException, InterruptedException {
		Iterator<PostingTagWeight> iterator = values.iterator();
		ArrayList<Long> postingIds =  new ArrayList<Long>();
		MapWritable tagsMap = new MapWritable();
		while(iterator.hasNext()){
			PostingTagWeight ptw = iterator.next();
			if(ptw.getPostingId().get() == KeyWordExtractionConstants.NEGONE){
				tagsMap = ptw.getTagMap();
			}
			else{
				postingIds.add(ptw.getPostingId().get());
			}
		}
		
		for(Long postId:postingIds){
			context.write(new LongWritable(postId), tagsMap);
		}

	}

}
