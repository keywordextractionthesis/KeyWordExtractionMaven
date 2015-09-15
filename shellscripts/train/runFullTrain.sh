source /home/ec2-user/keyword/application.properties
rm KeyWordExtraction*
hadoop fs -copyToLocal /user/ec2-user/keywordextract/Key* .
hadoop jar $jarName org.csulb.edu.keywordextraction.train.KeyWordExtractionDriver -D regex.record.separator=$regexFormat $basePath/input/fulltrain $basePath/output/fulltrain $cachePath
