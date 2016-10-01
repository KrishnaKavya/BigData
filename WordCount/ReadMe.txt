# Word Count using Hadoop MapReduce.

#execution Steps:

1. export the jar.
2. Login to the hadoop cluster and paste it.
3. Remove already existing output file: hdfs dfs -rmr output(output directory(kkkkkk/output))
4. Add input file to hdfs: hdfs dfs -put inputfilename /path/input(eg: kkkkkkk/input). The inout file is added to the input directory.
5. Execution of jar: hadoop jar wordcount.jar WordCount /kkkkk/input kkkkkk/output.( hadoop jar jarfileName className inputfilepath outputFilePath.
6. Check the output: hdfs dfs -cat kkkkkk/output/* (hdfs dfs -cat outputFilePath/*)
