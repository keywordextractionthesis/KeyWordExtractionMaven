package org.csulb.edu.keywordextraction.util;

import java.util.Map;

public class Utility {
	public static String[] fetchTopKTags(Map<String,Double> tokenMap, int k) {
		System.out.println("Token Map : "+tokenMap);
		double[] topValue = new double[k];
		String[] topTags = new String[k];
		int index;
		for (String key : tokenMap.keySet()) {
			double value = tokenMap.get(key);
			if(value > topValue[k-1]) {
				topValue[k-1] = value;
				topTags[k-1] = key;
				index = k-1;
				while (index > 0) {
					if (topValue[index] > topValue[index-1]){
						double temp = topValue[index];
						topValue[index] = topValue[index-1];
						topValue[index-1] = temp;	
						
						String tempStr = topTags[index];
						topTags[index] = topTags[index-1];
						topTags[index-1] = tempStr;	
					}
					index--;
				}
			}
		}
		System.out.print("Top 5 tags....");
		for(String s : topTags){
			System.out.print(s+" ");
		}
		System.out.println();
		return topTags;
	}
}
