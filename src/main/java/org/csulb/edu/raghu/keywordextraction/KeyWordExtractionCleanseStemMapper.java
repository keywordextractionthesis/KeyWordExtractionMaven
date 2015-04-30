package org.csulb.edu.raghu.keywordextraction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyWordExtractionCleanseStemMapper extends Mapper<LongWritable,Text,Text,Text> {

}
