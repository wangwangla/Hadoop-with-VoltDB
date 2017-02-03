# Hadoop-with-VoltDB
Hadoop Map-Reduce運算時, 各台worker可以in-memory取得voltDB資料<br>
增加Map-Reduce彈性(突破Key-Value限制)並加強運算效率(in-memory distributed new SQL)

# How To use
(1) 裝VoltDB
java 要 1.8 以上
$ sudo apt-get -y install ant build-essential ant-optional default-jdk python cmake valgrind ntp ccache git-arch git-completion git-core git-svn git-doc git-email python-httplib2 python-setuptools python-dev apt-show-versions
$ wget https://github.com/VoltDB/voltdb/archive/voltdb-6.0.1.tar.gz
$ tar -zxf voltdb-6.0.1.tar.gz
$ mv voltdb-voltdb-6.0.1 voltdb
$ cd voltdb
$ ant clean ; ant -Djmemcheck=NO_MEMCHECK
$ export CLASSPATH="$CLASSPATH:$HOME/voltdb/voltdb/*:$HOME/voltdb/lib/*:./"
$ alias voltdb='/home/yenkuanlee/voltdb/bin/sqlcmd'
$ /home/yenkuanlee/voltdb/bin/voltdb create --background     #啟動DB
$ voltdb
(2) 裝Hadoop
http://www.jerrynest.com/install-hadoop-2-6-0-on-ubuntu14-04/
(3)put jar to hdfs
$ hadoop fs -put ~/voltdb/voltdb/voltdb-6.0.1.jar .
$ hadoop fs -put ~/voltdb/voltdb/voltdbclient-6.0.1.jar .
(4)add conf in Java
conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdb-6.0.1.jar");
conf.set("tmpjars","/user/yenkuanlee/voltdb-6.0.1.jar");
conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdbclient-6.0.1.jar");
conf.set("tmpjars","/user/yenkuanlee/voltdbclient-6.0.1.jar");
(5) Coding
'''
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
public class WordCount {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
public void DoQuery(String ip,String sql){
                Client myApp;
                myApp = ClientFactory.createClient();
                try {
                        myApp.createConnection(ip);
                        myApp.callProcedure("@AdHoc",sql);
                        return ;
                } catch (Exception e) {
                        e.printStackTrace();
                        return ;
                }
        }
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    String thisWord = itr.nextToken();
        word.set(thisWord);
        context.write(word, one);
    DoQuery("192.168.122.39","insert into ooo values('"+thisWord+"');");
      }
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdb-6.0.1.jar");
    conf.set("tmpjars","/user/yenkuanlee/voltdb-6.0.1.jar");
    conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdbclient-6.0.1.jar");
    conf.set("tmpjars","/user/yenkuanlee/voltdbclient-6.0.1.jar");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
'''
