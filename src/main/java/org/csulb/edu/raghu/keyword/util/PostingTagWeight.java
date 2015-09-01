package org.csulb.edu.raghu.keyword.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class PostingTagWeight implements WritableComparable {

	private LongWritable postingId = new LongWritable(-1);
	private MapWritable tagMap=null;
	
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
