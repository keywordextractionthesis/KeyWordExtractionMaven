package org.csulb.edu.keywordextraction.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.PostingTagWeight;

public class KeyWordExtractionCompositeTokenReducer extends Reducer<Text, PostingTagWeight, LongWritable, Text> {

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
		Iterator<Entry<Writable, Writable>> itr = tagsMap.entrySet().iterator();
		StringBuilder sbr =  new StringBuilder();
		while(itr.hasNext()){
			Entry<Writable, Writable> tagValue = itr.next();
			sbr.append(((Text)tagValue.getKey()).toString());
			sbr.append(":");
			sbr.append(((DoubleWritable)tagValue.getValue()).get());
		}
		for(Long postId:postingIds){
			context.write(new LongWritable(postId), new Text(sbr.toString()));
		}

	}

}
