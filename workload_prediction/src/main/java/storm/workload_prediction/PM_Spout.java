package storm.workload_prediction;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class PM_Spout extends BaseRichSpout{

	SpoutOutputCollector _collector;
	static String tenant_id = "56fc364c204043b98a438122568fbf14";
	int pre_time;
	int cur_time;
	/*获取token*/
	public static String getToken() throws Exception {
		String urlPath = "HTTP://166.111.143.220:5000/v2.0/tokens";
		String json="";
		
		URL url = new URL(urlPath);  
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();  
		
		 
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		
		String content = "{\"auth\": {\"tenantName\": \"admin\", \"passwordCredentials\": {\"username\": \"admin\", \"password\":\"cloud\"}}}";
		
		DataOutputStream out = new DataOutputStream(connection.getOutputStream());
		
		out.writeBytes(content);
		out.flush();
		out.close();
		
		
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputline = null;
        while((inputline = in.readLine())!=null)
        {
        	json = json+inputline;
        }
        
       //解析json
        //System.out.println(json);
        JSONObject j =JSONObject.fromObject(json);
        JSONObject access =JSONObject.fromObject(j.get("access").toString());
        JSONObject token = JSONObject.fromObject(access.get("token").toString());
        String id = token.get("id").toString();
        //System.out.println(id);
  
        return id;
       
	}
	
	/*解析包含VM信息的json语句*/
	public static void VM_parse(String input,ArrayList<VM> VM_List)
	{
		try {
			
			JSONObject obj = JSONObject.fromObject(input);		
			
			JSONArray severs = obj.getJSONArray("hypervisors");
			for (int i = 0; i < severs.size(); i++){
				
				JSONObject element = severs.getJSONObject(i);
				String id = element.get("id").toString();
				String name = element.get("hypervisor_hostname").toString();

				VM vm = new VM();
				vm.id = id;
				vm.name = name;
				VM_List.add(vm);
			}
			
		}catch (Exception e) {   
            e.printStackTrace();   
        }
	}
	/*获取VM列表*/
	public void get_VM(String token,ArrayList<VM> VM_List) throws IOException
	{
		String urlPath = "http://166.111.143.220:8774/v2/"+tenant_id+"/os-hypervisors";
		String json = "";
		
		URL url = new URL(urlPath);  
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();  
		
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setRequestProperty("X-Auth-Token", token);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

		String inputline = null;
        while((inputline = in.readLine())!=null)
        {
        	json = json + inputline;
        	//System.out.println(inputline);
        }

        VM_parse(json,VM_List);

	}
	/*获取当前时间*/
	public static int get_current_time(){
		int current=0;
		Date current_date = new Date();
		int hour = current_date.getHours();
		int minute = current_date.getMinutes();
		int add=0;
		if(minute>=30){
			add=1;
		}
		current = 2*hour+add;
		return current;
	}
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		pre_time = -1;
	}
	
	/*查询数据 此节点负责查询内存数据*/
	public void nextTuple() {
		// TODO Auto-generated method stub
		
		cur_time = get_current_time();
		if(pre_time!=cur_time)
		{
		pre_time = cur_time;
		String token = "";//查询数据需要的
		
		//获取token
		try {
			 token = getToken();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//获取VM列表
		ArrayList<VM> VM_List = new ArrayList<VM>();
		try {
			get_VM(token,VM_List);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i=0;i<VM_List.size();i++){
			_collector.emit(new Values(VM_List.get(i).name,VM_List.get(i).id,token,get_current_time()));
		}
		
		
		}
		
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("name","id","token","current_time"));//PM信息以及查询所需的token
	}

}
