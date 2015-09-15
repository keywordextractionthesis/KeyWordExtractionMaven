source /home/ec2-user/keyword/application.properties
rm KeyWordExtraction*
hadoop fs -copyToLocal /user/ec2-user/keywordextract/Key* .
hadoop jar $jarName org.csulb.edu.keywordextraction.tagscount.TagFrequencyDriver -D regex.record.separator=$regexFormat $basePath/input/fulltrain $basePath/output/tagscount
