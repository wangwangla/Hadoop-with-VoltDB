import java.util.ArrayList;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
 
public class Initial {
	public static void main(String[] args) throws Exception {
		DropAllTable();
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
	public static void DropAllTable(){
		ArrayList<String> tables = ShowTables("localhost");
		for(int i=0;i<tables.size();i++){
			String tb = tables.get(i);
			DoQuery("DROP TABLE "+tb+";");
		}
		DoQuery("create table status(s int)");
		DoQuery("insert into status values(1);");
   }
	
	public static ArrayList<String> ShowTables(String ip){
		ArrayList<String> TableList = new ArrayList<String>();
		Client myApp;
		VoltTable[] results = {};
		myApp = ClientFactory.createClient();
		try {
			myApp.createConnection(ip);
			results = myApp.callProcedure("@SystemCatalog","TABLES").getResults();
		} catch (Exception e) {
			//e.printStackTrace();
			//return results;
		}
		try {
		String[] tmp = results[0].toJSONString().split("\"data\"")[1].split("],");
		for(int i=0;i<tmp.length;i++)
			TableList.add(tmp[i].split(",")[2].replace("\"",""));
		} catch (Exception e) {
			//e.printStackTrace();
			//return results;
		}
		return TableList;
	}
}
 

