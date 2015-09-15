source /home/ec2-user/keyword/application.properties
rm KeyWordExtraction*
hadoop fs -copyToLocal /user/ec2-user/keywordextract/Key* .
hadoop jar $jarName org.csulb.edu.keywordextraction.test.KeyWordExtractionTestDriverPhase1 -D regex.record.separator=$regexFormat $basePath/input/sampletest $basePath/output/fulltrain $basePath/output/sampletestphase1 $cachePath
