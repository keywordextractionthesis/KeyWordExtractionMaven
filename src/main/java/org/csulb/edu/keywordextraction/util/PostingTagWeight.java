package org.csulb.edu.keywordextraction.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.csulb.edu.keywordextraction.test.KeyWordExtractionCompositeTokenReducer;

public class PostingTagWeight implements WritableComparable {
	
	private static final Log LOG= LogFactory.getLog(PostingTagWeight.class);
	private LongWritable postingId;
	private MapWritable tagMap;

	public PostingTagWeight() {
	}
	
	public void set(PostingTagWeight ptw){
		this.postingId = ptw.getPostingId();
		this.tagMap = ptw.getTagMap();
	}
	
	public PostingTagWeight(LongWritable postingId, MapWritable tagMap){
		this.postingId=postingId;
		this.tagMap=tagMap;
	}
	
	public LongWritable getPostingId() {
		return postingId;
	}

	public void setPostingId(LongWritable postingId) {
		this.postingId = postingId;
	}

	public MapWritable getTagMap() {
		return tagMap;
	}

	public void setTagMap(MapWritable tagMap) {
		this.tagMap = tagMap;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		if(postingId == null){
			postingId = new LongWritable();
		}
		if(tagMap == null){
			tagMap= new MapWritable();
		}
		postingId.readFields(dataInput);
		tagMap.readFields(dataInput);
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		postingId.write(dataOutput);
		tagMap.write(dataOutput);
	}

	
	@Override
	public int compareTo(Object object) {
		PostingTagWeight other = (PostingTagWeight)object;
		return getPostingId().compareTo(other.getPostingId());
	}

}
