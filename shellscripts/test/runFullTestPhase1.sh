source /home/ec2-user/keyword/application.properties
rm KeyWordExtraction*
hadoop fs -copyToLocal /user/ec2-user/keywordextract/Key* .
hadoop jar $jarName org.csulb.edu.keywordextraction.test.KeyWordExtractionTestDriverPhase1 -D regex.record.separator=$regexFormat $basePath/input/fulltest $basePath/output/fulltrain $basePath/output/fulltestphase1 $cachePath
