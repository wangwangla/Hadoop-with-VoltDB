import os
os.system("hadoop fs -rmr output")
os.system("hadoop com.sun.tools.javac.Main WordCount.java")
os.system("jar cf wc.jar WordCount*.class")
os.system("hadoop jar wc.jar WordCount input output")
