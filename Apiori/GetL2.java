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

public class GetL2 {
public static final int minSup = 2;

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
//context.write(new Text(value.toString().split("\t")[0]), new IntWritable(1));

	String Sval = value.toString().split("\t")[0];
	int val = Integer.parseInt(Sval);
///*=======================================================================================
		Client myApp;
		myApp = ClientFactory.createClient();
		VoltTable[] results = {};
		ArrayList<Integer> L1 = new ArrayList<Integer>();
		try {
			myApp.createConnection("localhost");
			results = myApp.callProcedure("@AdHoc","select * from L1 where I1 > "+Sval+";").getResults();
			for(int i=0;i<results[0].getRowCount();i++){
				int Vtmp = (int)results[0].fetchRow(i).getLong(0);
				String Stmp = String.valueOf(Vtmp);
				if(IfFP2(Sval,Stmp))
					context.write(new Text(Sval), new IntWritable(Vtmp));
			}
		} catch (Exception e) {
			context.write(new Text("kevin"), new IntWritable(1));
			e.printStackTrace();
		}
//=======================================================================================*/

/*=======================================================================================
	ArrayList<Integer> aa = GetL1();
    	for(int i=0;i<aa.size();i++){
		if(val < aa.get(i)){
			if(IfFP2(Sval,String.valueOf(aa.get(i))))
				context.write(new Text(Sval), new IntWritable(aa.get(i)));
		}
    	}
=======================================================================================*/

    }
  }

  public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

	DoQuery("create table I"+key.toString()+" (ID int not null,PRIMARY KEY (ID));PARTITION TABLE I"+key.toString()+" ON COLUMN ID;");
      int sum = 0;
	ArrayList<Object[]> Ivalue = new ArrayList<Object[]>();
      for (IntWritable val : values) {
        sum += 1;
	Object [] a = {key.toString(),val.toString()};
	Ivalue.add(a);
      }
	InsertData("L2",Ivalue);
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

	ArrayList<Object[]> Ivalue = new ArrayList<Object[]>();
      for (IntWritable val : values) {
	Object [] a = {key.toString(),val.toString()};
	Ivalue.add(a);
	CreateIntersectionTable(key.toString(),val.toString());
        context.write(key, val);
      }
	InsertData("L2",Ivalue);
    }
  }

  public static void main(String[] args) throws Exception {
    DoQuery("update status set s=s+1;");
    CreateLn();
    Configuration conf = new Configuration();
    conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdb-6.0.1.jar");
    conf.set("tmpjars","/user/yenkuanlee/voltdb-6.0.1.jar");
    conf.set("tmpjars","/home/yenkuanlee/voltdb/voltdb/voltdbclient-6.0.1.jar");
    conf.set("tmpjars","/user/yenkuanlee/voltdbclient-6.0.1.jar");
    
    Job job = Job.getInstance(conf, "get L2");
    job.setJarByClass(GetL2.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

public static boolean IfFP2(String a,String b){
//    	String sql = "select COUNT(*) from i"+a+",i"+b+" where i"+a+".id=i"+b+".id;";
    	String sql = "select COUNT(id) from i"+a+" where id in (select id from i"+b+");";
        Client myApp;
		myApp = ClientFactory.createClient();
		try {
			myApp.createConnection("localhost");
			long r = myApp.callProcedure("@AdHoc",sql).getResults()[0].fetchRow(0).getLong(0);
			if(r>=minSup)return true;
			return false;

		} catch (Exception e) {
			//e.printStackTrace();
			return false;
		}
	}

public static ArrayList<Integer> GetL1(){
		Client myApp;
		myApp = ClientFactory.createClient();
		VoltTable[] results = {};
		ArrayList<Integer> L1 = new ArrayList<Integer>();
		try {
			myApp.createConnection("localhost");
			results = myApp.callProcedure("@AdHoc","select * from L1;").getResults();
			for(int i=0;i<results[0].getRowCount();i++)
				L1.add((int)results[0].fetchRow(i).getLong(0));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return L1;
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

public static void CreateIntersectionTable(String a,String b){
		ArrayList<Object[]> Ivalue = new ArrayList<Object[]>();
		String table_name = "i_"+a+"_"+b;
		DoQuery("create table "+table_name+"(id int,unique (id));");
		Client myApp;
        myApp = ClientFactory.createClient();
        VoltTable[] results = {};
        try {
                myApp.createConnection("localhost");
                results = myApp.callProcedure("@AdHoc","select id from i"+a+" intersect (select id from i"+b+");").getResults();
                for(int i=0;i<results[0].getRowCount();i++){
                	Object [] tmp = {String.valueOf(results[0].fetchRow(i).getLong(0))};
                	Ivalue.add(tmp);
                }
                InsertData(table_name,Ivalue);

        } catch (Exception e) {
                e.printStackTrace();
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
//=============================================================================================




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
		String sql = "create table L"+String.valueOf(s)+" ("+schema+");";
		try {
			myApp.createConnection("localhost");
			myApp.callProcedure("@AdHoc",sql);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
    }
}
