package org.csulb.edu.raghu.keyword.keywordtesting;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.csulb.edu.raghu.keyword.util.PostingTagWeight;

public class KeyWordExtractionCompositeTokenReducer extends Reducer<Text, PostingTagWeight, Text, MapWritable> {

}
