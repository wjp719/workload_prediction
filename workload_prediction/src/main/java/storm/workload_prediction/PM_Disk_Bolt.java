package storm.workload_prediction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PM_Disk_Bolt implements IRichBolt{

	OutputCollector _collector;
	int sample_num ;//每天采样48个
	int period_num;//需要7天的数据
	String meter_name;
	String[] time = {"00:00:00","00:30:00","01:00:00","01:30:00","02:00:00","02:30:00","03:00:00","03:30:00","04:00:00","04:30:00","05:00:00","05:30:00","06:00:00","06:30:00","07:00:00","07:30:00","08:00:00","08:30:00","09:00:00","09:30:00","10:00:00","10:30:00","11:00:00","11:30:00","12:00:00","12:30:00","13:00:00","13:30:00","14:00:00","14:30:00","15:00:00","15:30:00","16:00:00","16:30:00","17:00:00","17:30:00","18:00:00","18:30:00","19:00:00","19:30:00","20:00:00","20:30:00","21:00:00","21:30:00","22:00:00","22:30:00","23:00:00","23:30:00"};//时间00:00：00-23:30:00，间隔30分钟
	double[][] original_data;//查询到的未经加工的历史数据
	double[] avg;//平均值
	double[] current_data;//当前数据
	double[] predicted_data;//预测结果
	

	public static double[] parse(String protocolXML,String date) {   
		double[] data = new double[48];
	 try { 
	    	 
	         DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();   
	         DocumentBuilder builder = factory.newDocumentBuilder();   
	         Document doc = builder.parse(new InputSource(new StringReader(protocolXML)));   

	         Element result = doc.getDocumentElement();//result   
	         NodeList items = result.getChildNodes();//items
	         String item_period_start = "";
	         String item_avg = "";
	         boolean finish = false;
	         double first_data = 0;
	         boolean first = true;
	         String[] time = {"00:00:00","00:30:00","01:00:00","01:30:00","02:00:00","02:30:00","03:00:00","03:30:00","04:00:00","04:30:00","05:00:00","05:30:00","06:00:00","06:30:00","07:00:00","07:30:00","08:00:00","08:30:00","09:00:00","09:30:00","10:00:00","10:30:00","11:00:00","11:30:00","12:00:00","12:30:00","13:00:00","13:30:00","14:00:00","14:30:00","15:00:00","15:30:00","16:00:00","16:30:00","17:00:00","17:30:00","18:00:00","18:30:00","19:00:00","19:30:00","20:00:00","20:30:00","21:00:00","21:30:00","22:00:00","22:30:00","23:00:00","23:30:00"};//时间00:00：00-23:30:00，间隔30分钟
	     	 
	         
	         ArrayList<rec_data> d = new ArrayList<rec_data>();
	         
	         
	         if (items.item(0) != null) {   
	         	//System.out.println(items.getLength());
	             for (int i = 0; i < items.getLength(); i++) {//对所有item
	             		//System.out.println((i+1) +". "+items.item(i).getNodeName());
	             		
	             	NodeList item_content = items.item(i).getChildNodes();//每个item里的内容

	             	int j;
	             	for(j = 0;j<item_content.getLength();j++)
	             	{
	             		if(item_content.item(j).getNodeName()=="period_start")
	             		{
	             			//System.out.println(item_content.item(j).getNodeName()+" "+item_content.item(j).getFirstChild().getNodeValue());
	             			item_period_start = item_content.item(j).getFirstChild().getNodeValue();
	                 	}
	             		if(item_content.item(j).getNodeName()=="avg")
	             		{
	             			//System.out.println(item_content.item(j).getNodeName()+" "+item_content.item(j).getFirstChild().getNodeValue());
	             			item_avg = item_content.item(j).getFirstChild().getNodeValue();
	             		}
	             	}
	             	rec_data r = new rec_data();
	             	r.avg = Double.parseDouble(item_avg);
	             	r.date = item_period_start;
	             	d.add(r);
	              }
		         int count =0;
		         for(int i = 0;i<48;i++){
		        	 String s = date+"T"+time[i];
		        	 if(s.equals(d.get(count).date)){
		        		 //System.out.println(count);
		        		data[i] = d.get(count).avg;
		        		count++;
		        		if(count>=items.getLength()){
		        			//System.out.println("!");
		        			for(i=i+1;i<48;i++){
		        				data[i] = data[i-1];
		        			}
		        			break;
		        		}
		        			
		        	 }
		        	 else{
		        		 data[i]=-1;
		        	 }
		         }
		         make_data(data);
		         for(int i = 0;i<48;i++){
		        	 //System.out.println(i+" "+data[i]);
		         }  
	          }  else
	          {data[0] = -2;} 

	           
	     } catch (Exception e) {   
	         e.printStackTrace();   
	     }
	 return data;
	 
	 }  
	
/*将parse方法中返回数据中缺少的数据(-1占位)补齐，原理如下：
 *…… x,-1,-1,y……
 *补全为
 **…… x,(y-x)/3,(y-x)*2/3,y……*/
static void make_data(double[] data){
	int count = 0;
	double pre=0;
	boolean flag =false;
	/*处理前N个时间断都没有数据的情况*/
	boolean continuous = false;
	int start=0;
	int finish=0;
	for(int i =0;i<48;i++){
		if(data[0]==-1&&continuous==false){
			start = 0;
			continuous = true;
		}else if(data[i]!=-1&&continuous==true){
			finish = i;
			break;
		}
	}
	
	for(int i =start;i<finish;i++){
		data[i] = data[finish];
	}
	//System.out.println("start:"+start+",finish:"+finish);
	
	if(data[0]==-1){
		
		data[0] = data[1];
	}
	
	for(int i =1;i<48;i++){
		
		if(data[i]==-1&&flag==false){
			count++;
			pre = data[i-1];
			flag = true;
		}
		else if(data[i]==-1&&flag==true){
			count++;
		}
		if(data[i]!=-1&&flag==true){
			double fractor = (data[i]-pre)/(count+1);
			for(int j = 1;j<=count;j++){
				data[i-j] = data[i]-fractor*count*j;
			}
			count=0;
			flag = false;
		}
	}
}
	public static double[] get_data(String token,String urlPath,String date)  throws Exception {
		double[] data = new double[48];
			
			String json = "";
			
			URL url = new URL(urlPath);  
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();  
			
			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("X-Auth-Token", token);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			//FileOutputStream out =  new FileOutputStream(new File("meters.txt"));

			String inputline = null;
	        while((inputline = in.readLine())!=null)
	        {
	        	json = json + inputline ;
	        	//System.out.println(inputline);
	        }
	        
	      //  out.write(json.getBytes());
	      //  out.close();
	        
	        return parse(json,date);
	       // return parse_PM_CPU(json);
		}

	/*求历史数据平均值
	 * 如果*/
	public void calculate_avg(double[][] his_data,double[] avg)
	{
		for(int i = 0;i<48;i++){
			double sum = 0;
			int count = 0;
			for(int j = 0;j<7;j++){
				if(his_data[j][0]!=-2&&his_data[j][i]!=-1){
				count++;
				sum = sum + his_data[j][i];
				}
			}
			if(count!=0){
			avg[i] = sum/count;
			}else{
				avg[i] = -1;//count=0说明没有历史数据无从计算平均值
			}
		}
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		sample_num = 48;//每天采样48个
		period_num = 7;//需要7天的数据
		meter_name = "PM_disk";
		original_data = new double[7][];
		for(int i = 0;i<period_num;i++){
			original_data[i] = new double[sample_num];
		}
		avg = new double[48];
		predicted_data= new double[48];//预测结果
	}
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		try { 
			String name = input.getStringByField("name");
			String id = input.getStringByField("id");
			String token = input.getStringByField("token");
			int current_time = (Integer) input.getValueByField("current_time");
			/*查询数据*/
			String time_condition0 = "";
			String time_condition1 = "";
			String url = "";
			String url_pre = "http://166.111.143.220:8777/v2/meters/hardware.disk.size.used/statistics?";
			String condition_id = "q.field=resource_id&q.op=eq&q.value=";
			String conditon_perod="period=1800";//查询周期 半小时
			
			Date current_date = new Date();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");//设置日期格式
			/*查询历史数据 */
			for(int j = 0;j<period_num;j++){
				Date temp = new Date(current_date.getTime() - (long)(j+1) * 24 * 60 * 60 * 1000);
				Date temp2 = new Date(current_date.getTime() - (long)(j+2) * 24 * 60 * 60 * 1000);
				String date = df.format(temp);
				String date2 = df.format(temp2);
				
					time_condition0 = "q.field=timestamp&q.op=ge&q.value="+date2+"T00:00:00";//开始时间
					time_condition1 = "q.field=timestamp&q.op=lt&q.value="+date+"T00:00:00";//终止时间
					
					url = url_pre+condition_id+name+"&"+time_condition0+"&"+time_condition1+"&"+conditon_perod;
					//System.out.println(url);
					original_data[j]= get_data(token,url,date2);

					//System.out.println(name+" "+date+":"+Arrays.toString(original_data[j]));				
			}
			//计算平均值
			calculate_avg(original_data,avg);
			//查询当天历史数据
			if(avg[0]!=-1){//avg=0说明没有查询到关于该指标的历史数据，无法计算平均值
				url = url_pre+condition_id+id+"&"+"q.field=timestamp&q.op=ge&q.value="+df.format(current_date)+"T00:00:00"+"&"+"q.field=timestamp&q.op=lt&q.value="+df.format(new Date(current_date.getTime() + (long) 24 * 60 * 60 * 1000))+"T00:00:00"+"&"+conditon_perod;
				current_data = get_data(token,url,df.format(current_date));
				System.out.println(name+" avg:"+Arrays.toString(avg));
			
			
			//处理查询结果
			double previous_value = 0;
			for(int j = 0;j<sample_num;j++){
				
				
				//获取预测值
				if(j==0)
				{
					predicted_data[j] = -1;
				}else if(j>=1 &&j<current_time)
				{
					double sub = avg[j] -avg[j-1];
					predicted_data[j] = current_data[j-1]+sub;
					previous_value = predicted_data[j];
				}else if(j>=current_time)
				{
					double sub = avg[j] - avg[j-1];
					predicted_data[j] = previous_value + sub;
					previous_value = predicted_data[j];
				}
				if( predicted_data[j]<0){
					 predicted_data[j]=0;
				}			
				//System.out.println(m.VM_name+"["+j+"]"+data.predicted_value);
			}			
			
			_collector.emit(new Values(id,name,meter_name,current_time,predicted_data,current_data,avg));
		
			}
	
                
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		/*定义元组格式
		 * id: 物理机di
		 * name:物理机名
		 * current_time:预测数据产生的时间
		 * preditcted_data:预测结果
		 * actual_value:真实的历史数据
		 * avg:历史数据平均值
		 * */
		declarer.declare(new Fields("id","name","meter","current_time","preditcted_data","actual_value","avg"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
