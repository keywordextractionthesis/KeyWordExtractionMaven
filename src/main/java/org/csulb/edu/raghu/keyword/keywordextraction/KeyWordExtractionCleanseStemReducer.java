package org.csulb.edu.raghu.keyword.keywordextraction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.raghu.keyword.keywordextraction.KeyWordExtractionDriver.CUSTOMCOUNTERS;

public class KeyWordExtractionCleanseStemReducer extends Reducer<Text,MapWritable,Text,MapWritable> {

	long postingsCount;
	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		long documentFrequency = 0;
		MapWritable tagMapWritable = new MapWritable();
		Iterator<MapWritable> iterator = values.iterator();
		
		while(iterator.hasNext()){
			documentFrequency++;
			MapWritable tagMap = iterator.next();
			Iterator<Entry<Writable, Writable>> tagMapIterator = tagMap.entrySet().iterator();
			while(tagMapIterator.hasNext()){
				Entry<Writable, Writable> tagValue = tagMapIterator.next();
				if(!tagMapWritable.containsKey(tagValue.getKey())){
					tagMapWritable.put(tagValue.getKey(), tagValue.getValue());
				}
				else{
					double value1= ((DoubleWritable)tagMapWritable.get(tagValue.getKey())).get();
					double value2 = ((DoubleWritable)tagValue.getValue()).get();
					double finalValue = value1+value2;
					tagMapWritable.put(tagValue.getKey(), new DoubleWritable(finalValue));
				}
				tagMapIterator.remove();
			}
		}
		
		double Ndf = Math.log10(postingsCount/documentFrequency);
		
		Iterator<Entry<Writable, Writable>> tagMapWritableIterator = tagMapWritable.entrySet().iterator();
		while(tagMapWritableIterator.hasNext()){
			Entry<Writable, Writable> tagValue = tagMapWritableIterator.next();
			double tfidf = ((DoubleWritable)tagValue.getValue()).get() * Ndf;
			tagMapWritable.put(tagValue.getKey(), new DoubleWritable(tfidf));
		}
		
		context.write(key, tagMapWritable);
	}

	@Override
	protected void setup(Reducer<Text, MapWritable, Text, MapWritable>.Context context)
			throws IOException, InterruptedException {
		postingsCount = context.getCounter(CUSTOMCOUNTERS.TOTAL_POSTINGS).getValue();		
	}

	
}
