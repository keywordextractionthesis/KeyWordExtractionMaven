package org.csulb.edu.raghu.keyword.keywordtesting;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.raghu.keyword.util.PostingTagWeight;

public class KeyWordExtractionTestTokenizeTagMapper extends Mapper<Text, MapWritable, Text, PostingTagWeight> {

}
