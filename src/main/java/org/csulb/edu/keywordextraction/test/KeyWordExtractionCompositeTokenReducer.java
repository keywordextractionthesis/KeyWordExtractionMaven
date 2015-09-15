package org.csulb.edu.keywordextraction.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.keywordextraction.util.KeyWordExtractionConstants;
import org.csulb.edu.keywordextraction.util.PostingTagWeight;

public class KeyWordExtractionCompositeTokenReducer extends Reducer<Text, PostingTagWeight, LongWritable, MapWritable> {

	PostingTagWeight ptw;
	Iterator<PostingTagWeight> iterator;
	ArrayList<LongWritable> postingIds;
	MapWritable tagsMap;
	Iterator<Entry<Writable, Writable>> itr;
	StringBuilder sbr;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		ptw = new PostingTagWeight();
	}

	@Override
	protected void reduce(Text key, Iterable<PostingTagWeight> values, Context context)
			throws IOException, InterruptedException {

		iterator = values.iterator();
		postingIds = new ArrayList<LongWritable>();
		tagsMap = new MapWritable();
		itr = tagsMap.entrySet().iterator();
		sbr = new StringBuilder();

		while (iterator.hasNext()) {
			ptw.set(iterator.next());

			if (ptw.getPostingId().get() == KeyWordExtractionConstants.NEGONE) {
				tagsMap = ptw.getTagMap();

			} else {
				postingIds.add(ptw.getPostingId());

			}
		}

		// while (itr.hasNext()) {
		// Entry<Writable, Writable> tagValue = itr.next();
		// sbr.append(((Text) tagValue.getKey()).toString());
		// sbr.append(":");
		// sbr.append(((DoubleWritable) tagValue.getValue()).get());
		// }
		for (LongWritable postId : postingIds) {
			context.write(postId, tagsMap);
		}

	}

}
