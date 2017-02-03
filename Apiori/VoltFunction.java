import java.io.IOException;
import java.util.ArrayList;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.VoltTable;
public class VoltFunction {
	public static int minSup = 2;

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
		//String sql = "create table L"+String.valueOf(s)+" (";
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
}
