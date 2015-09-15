source /home/ec2-user/keyword/application.properties
rm KeyWordExtraction*
hadoop fs -copyToLocal /user/ec2-user/keywordextract/Key* .
hadoop jar $jarName org.csulb.edu.keywordextraction.test.KeyWordExtractionTestDriverPhase2 -D regex.record.separator=$regexFormat $basePath/output/sampletestphase1 $basePath/output/sampletestphase2 $cachePath
