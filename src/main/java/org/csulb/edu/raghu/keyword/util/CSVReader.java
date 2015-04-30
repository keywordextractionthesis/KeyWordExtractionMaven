package org.csulb.edu.raghu.keyword.util;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class CSVReader {
	
	static List<Posting> postingsList = new ArrayList<>();
	
	
	public static void main(String[] args) {
		//generateStopWords();
		//System.out.println("Stop Words set size : "+stopWords.size());
		getDataFromFile();
		System.out.println(postingsList.size());
		for(Posting p:postingsList)
			System.out.println(p);
	}
	
	
	public static void getDataFromFile(){
		try {
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\s_train.csv"));
			String str;
			int quesCount=0,count;
			boolean isFirstQuestion=true;
			StringBuffer record = new StringBuffer();
			String[] input;
			//Extract each line from the file
			while((str = br.readLine())!=null){
				
				//Split the input string using ","
				input = str.split(",");
				try {
					//If the line begins with the question id
					//Else throw a parser exception will be thrown
					count = Integer.parseInt(input[0]);
					//Applicable when the application is done as mapper program
					//Extract the first question id
					if(isFirstQuestion){
						quesCount=count-1;
					} 
					//Question Id's are sequential 
					if(count==quesCount+1){
						//System.out.println(record.toString());
						if(!isFirstQuestion){
							addToPostingsList(record.toString());					
						}
						isFirstQuestion=false;
						//Clear the buffer
						record.setLength(0);
						//Append the current string to the buffer
						record.append(str);
						//Update question Count 
						quesCount = count;
					}else{
						//If a line begins with a number and is not sequential then throw exception
						throw new Exception();
					}
				} catch (Exception e) {
					//Keep appending the lines to the buffer when parse exception is thrown
					record.append(str);
					
				}
			}
			//Add the last record
			addToPostingsList(record.toString());
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void addToPostingsList(String str){
		//System.out.println(str+"..");
		Posting newPost = new Posting(str);
		postingsList.add(newPost);
		System.out.println(newPost);
	}
	
	
	
}
