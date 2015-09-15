package org.csulb.edu.keywordextraction.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.Utility;

public class KeyWordExtractionQuestionCollectionReducer extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
	@Override
	protected void reduce(LongWritable key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<MapWritable> tagsIterator = values.iterator();
		Map<String, Double>tagsMap = new HashMap<String, Double>();
		while(tagsIterator.hasNext()){
			MapWritable tagMapWritable = tagsIterator.next();
			Iterator<Entry<Writable, Writable>> tagMapIterator = tagMapWritable.entrySet().iterator();
			while(tagMapIterator.hasNext()){
				Entry<Writable, Writable> tagValue = tagMapIterator.next();
				if(!tagsMap.containsKey(tagValue.getKey().toString())){
					tagsMap.put(tagValue.getKey().toString(), ((DoubleWritable)tagValue.getValue()).get());
				}
				else{
					double value1= tagsMap.get(tagValue.getKey().toString());
					double value2 = ((DoubleWritable)tagValue.getValue()).get();
					double finalValue = value1+value2;
					tagsMap.put(tagValue.getKey().toString(), finalValue);
				}
				tagMapIterator.remove();
			}
			
		}
		String[] topTags = Utility.fetchTopKTags(tagsMap, KeyWordExtractionConstants.FIVE);
		StringBuilder sbr =  new StringBuilder();
		for(String tag: topTags){
			if(tag != null){
			sbr.append(tag);
			sbr.append(KeyWordExtractionConstants.SPACE);
			}
		}
		
		String finalTags = sbr.length() > 0 ? sbr.substring(0, sbr.length() - 1): "";
//		StringBuilder sbr = new StringBuilder();
//		for(String tag:tagsMap.keySet()){
//			sbr.append(tag);
//			sbr.append(",");
//		}
//		String finalTags = sbr.toString();
		
		context.write(key, new Text(finalTags));
		}
	}


