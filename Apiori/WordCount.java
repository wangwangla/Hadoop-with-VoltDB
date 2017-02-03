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

import java.util.ArrayList;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.VoltTable;

public class WordCount {
public static final int minSup = 2;

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	String thisWord = itr.nextToken();
        word.set(thisWord);
        context.write(word, new IntWritable(key.hashCode()));// item, TID

      }
    }
  }

  public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
	//DoQuery("create table I"+key.toString()+" (ID int not null,PRIMARY KEY (ID));PARTITION TABLE I"+key.toString()+" ON COLUMN ID;");
	DoQuery("create table I"+key.toString()+" (ID int not null,unique (ID));");
      int sum = 0;
	ArrayList<Object[]> Ivalue = new ArrayList<Object[]>();
      for (IntWritable val : values) {
        sum += 1;
	Object [] a = {val.get()};
	Ivalue.add(a);
      }
	InsertData("I"+key.toString(),Ivalue);
      result.set(sum);
      context.write(key, result);
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
	ArrayList<Object[]> Ivalue = new ArrayList<Object[]>();
	if(sum <minSup){
		DoQuery("drop table I"+key.toString());
	}
	else{
		Object [] a = {Integer.parseInt(key.toString())};
		Ivalue.add(a);
		result.set(sum);
	        context.write(key, result);
	}
	InsertData("L1",Ivalue);
      //result.set(sum);
      //context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    CreateLn();
    Configuration conf = new Configuration();
    conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdb-6.0.1.jar");
    conf.set("tmpjars","/user/yenkuanlee/voltdb-6.0.1.jar");
    conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdbclient-6.0.1.jar");
    conf.set("tmpjars","/user/yenkuanlee/voltdbclient-6.0.1.jar");
    
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

public static void DoQuery(String sql){
                Client myApp;
                myApp = ClientFactory.createClient();

                try {
                        myApp.createConnection("localhost");
                        myApp.callProcedure("@AdHoc",sql);
                        return ;
                } catch (Exception e) {
                        e.printStackTrace();
                        return ;
                }
        }

public static void InsertData(String table_name,ArrayList<Object[]> rows){
		Client myApp;
		myApp = ClientFactory.createClient();
		try {
				myApp.createConnection("localhost");
				for(int i=0;i<rows.size();i++){
					myApp.callProcedure(table_name+".insert",rows.get(i));
				}
			return ;
		} catch (Exception e) {
			e.printStackTrace();
			return ;
		}
	}

public static int GetStatus(){
		Client myApp;
		myApp = ClientFactory.createClient();
		try {
			myApp.createConnection("localhost");
			return (int)myApp.callProcedure("@AdHoc","select * from status;").getResults()[0].fetchRow(0).getLong(0);
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
    
    public static void CreateLn(){
    	Client myApp;
		myApp = ClientFactory.createClient();
		int s = GetStatus();
		StringBuilder tmp = new StringBuilder();
		for(int i=1;i<=s;i++){
			tmp.append("I"+String.valueOf(i)+" int,");
		}
		String schema = tmp.toString();
		schema = schema.substring(0, schema.length()-1);
		String sql = "";
		//sql += "drop table L"+String.valueOf(s)+";";
		sql += "create table L"+String.valueOf(s)+" ("+schema+");";
		try {
			myApp.createConnection("localhost");
			myApp.callProcedure("@AdHoc",sql);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
    }
}
