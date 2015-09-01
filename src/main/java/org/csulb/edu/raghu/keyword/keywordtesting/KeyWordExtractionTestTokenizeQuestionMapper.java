package org.csulb.edu.raghu.keyword.keywordtesting;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.csulb.edu.raghu.keyword.util.PostingTagWeight;

public class KeyWordExtractionTestTokenizeQuestionMapper extends Mapper<LongWritable,Text,Text,PostingTagWeight> {

}
