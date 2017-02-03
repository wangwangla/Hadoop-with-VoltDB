import os
os.system("hadoop fs -rmr L2")
os.system("hadoop com.sun.tools.javac.Main GetL2.java")
os.system("jar cf get_L2.jar GetL2*.class")
os.system("hadoop jar get_L2.jar GetL2 output L2")
