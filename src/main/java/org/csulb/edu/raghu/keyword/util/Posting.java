package org.csulb.edu.raghu.keyword.util;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;


//Java class for identifying a posting
public class Posting {
	private long id;
	private String title;
	private String body;
	private List<String> tags;
	private String codeSection;
	private Map<String,Double> tokens;
	
	private Set<String> stopWords;
	private Path[] stopWordFiles;
	
	//Parameterized Constructor
	public Posting(Path[] stopWordFiles){
		this.stopWordFiles = stopWordFiles;
		this.stopWords = generateStopWords();		
	}
	
	public void processPosting(String record, boolean isTraining){
		try{
		tokens = new HashMap<>();
		String[] input = record.toString().split(",");
		int n = input.length;
		this.id = Long.parseLong(input[0].replaceAll("\"", ""));
		this.title = cleanString(input[1]);
		StringBuffer body = new StringBuffer();
		
		//For training data the last column is of the csv file is tags
		if(isTraining){
			for(int i=2;i<n-1;i++){
				body.append(input[i]);
			}
		}else{
			//Test data doesn't contain the tags column
			for(int i=2;i<n;i++){
				body.append(input[i]);
			}
		}
		StringBuffer codeSection = new StringBuffer();
		int startIndex,endIndex,loopCount,counter=0;
		 while(body.toString().contains("<code>")){
	            startIndex = body.indexOf("<code>");
	            endIndex = body.indexOf("</code>");
	            if (startIndex == -1 || endIndex == -1 || startIndex > endIndex){
	            	break;
	            }
	            String codeSubString = body.substring(startIndex+6, endIndex);           
	            while (codeSubString.contains("<code>")) {
	            	startIndex = startIndex + codeSubString.indexOf("<code>") + 6;
	            	codeSubString = body.substring(startIndex+6, endIndex);
	            }
	            codeSection.append(body.substring(startIndex, endIndex+7));
	            body = body.replace(startIndex, endIndex+7, "");
	        }
		if(codeSection.length()==0)
			this.codeSection = null;
		else
			this.codeSection = codeSection.toString();
		
		this.body = cleanString(body.toString());
		//Tokenization of the question title and body
		addToTokensMap(this.title);
		addToTokensMap(this.body);
		//Add tags to an array list if the input data set is training data set
		if(isTraining){
			String[] tags = input[n-1].split(" ");
			this.tags = new ArrayList<>();
			for(String tag:tags){
				this.tags.add(tag);
			}
		}else{
			//For test data there will not be any tags
			this.tags = null;
		}
		}catch(Exception e){
			System.out.println("EXECEPTION");
			e.printStackTrace();
		}
		}
	
	
	/*
	 *  
	 */
	public void addToTokensMap(String str){
		//Add the cleaned body to posting
		String[] splitTokens = str.split(" ");
		double count;
		Set<String> tokensSet = new HashSet<>(Arrays.asList(splitTokens));
		Set<String> stemmedTokens = CustomStemmer.stemTokens(tokensSet);
		for(String currentToken : stemmedTokens){
			if(!stopWords.contains(currentToken)){
				if(tokens.containsKey(currentToken)){
					count = tokens.get(currentToken);
					tokens.put(currentToken, count+1);
				}else{
					tokens.put(currentToken, 1.0);
				}
			}
		}
		computeLogWeightedFrequency();
	}
	
	public void computeLogWeightedFrequency() {
		for (String token : tokens.keySet()) {
			double termFrequency = tokens.get(token);
			double logWeightedFequency = 1 + Math.log10(termFrequency);
			tokens.put(token, logWeightedFequency);
		}
	}
	
	
	/*
	 * Method for performing the cleaning 
	 */
	public static String cleanString(String str){
		String cleanedStr;
		//Convert to lowercase, 
		cleanedStr = str.toLowerCase();
		//Remove all html tags 
		cleanedStr = cleanedStr.replaceAll("\\<.*?>","");
		//Remove special characters from body
		cleanedStr = cleanedStr.replaceAll("[\\-\\\"\\+\\^:,()?*]"," ");
		cleanedStr = cleanedStr.replaceAll("[\\.']","");
		//Trim additional white spaces
		cleanedStr = cleanedStr.trim();
		return cleanedStr;
	}
	
	/*
	 * Method that extracts stop words from text files and adds them to the stop words set
	 */
	public Set<String> generateStopWords(){
		Set<String> stopWordsSet = new HashSet<>();
		BufferedReader br;
		try {
			for (Path uri : stopWordFiles) {
				br = new BufferedReader(new FileReader(uri.toString()));
				String str;
				//Extract each line from the file
				while((str = br.readLine())!=null){
					stopWordsSet.add(str.toLowerCase());
				}
				br.close();
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return stopWordsSet;
	}
	
	public String toString(){
		//return id+": "+title+"\n"+body+"\n"+codeSection+"\n"+tags;
		return id+":"+tokens+"\n"+tags;
	}

	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getCodeSection() {
		return codeSection;
	}

	public void setCodeSection(String codeSection) {
		this.codeSection = codeSection;
	}

	public Map<String,Double> getTokens() {
		return tokens;
	}

	public void setTokens(Map<String,Double> tokens) {
		this.tokens = tokens;
	}	
	
	public void clear(){
		this.setId(0L);
		this.setTitle(null);
		this.setBody(null);
		this.setTags(null);
		this.setCodeSection(null);
		this.setTokens(null);
	}
}
