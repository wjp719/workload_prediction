package storm.workload_prediction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class predict_Bolt implements IRichBolt {

	OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		try {
			// "metrics", "instance", "token", "current_time"
			InfluxdbOP influxdbOP = new InfluxdbOP(1);
			String metric = input.getStringByField("metrics");
			Date current_date = new Date();
			//System.out.println(current_date.toGMTString());
			String instance = input.getStringByField("instance");;
			List<JSONArray> history_data=new ArrayList<JSONArray>();
			int period_num=7;
			for (int j = 0; j < period_num; j++) {
				Date start = new Date(current_date.getTime() - (long) (j + 1) * 24 * 60 * 60 * 1000 - 60 * 1000);
				Date end = new Date(start.getTime() + 30 * 60 * 1000);
				//System.out.println(current_date.toString());
				String cmd = "select MEAN(value) from \"" + metric + "\" where time>"
						+ String.valueOf(start.getTime() * 1000000) + " and time<" + String.valueOf(end.getTime() * 1000000)
						+ " and vm='" + instance + "'" + " GROUP BY time(1m)";
				//System.out.println(cmd);
				JSONArray jArray = influxdbOP.selectQuery(cmd);
				//System.out.println(jArray);
				if (jArray.size() > 0)
					history_data.add(jArray.getJSONObject(0).getJSONArray("value"));
				else {
					break;
				}
			}
			//System.out.println(history_data.toString());
			double[] avg=new double[35];
			int[] count=new int[35];
			for(int i=0;i<history_data.size();i++){
				JSONArray jsonArray=history_data.get(i);
				for(int j=0;j<jsonArray.size();j++){
					JSONObject object=jsonArray.getJSONObject(j);
					if(object.has("mean")){
						avg[j]+=object.getDouble("mean");
						count[j]++;
					}
				}
			}
			for(int i=0;i<avg.length;i++){
				if(count[i]!=0)
					avg[i]/=count[i];
			}
			//for(int i=0;i<avg.length;i++)
			//System.out.println(avg[i]);
			//predict
			Date start = new Date(current_date.getTime()  - 60 * 1000);
			Date end = current_date;
			//System.out.println(current_date.toString());
			String cmd = "select MEAN(value) from \"" + metric + "\" where time>"
					+ String.valueOf(start.getTime() * 1000000) + " and time<" + String.valueOf(end.getTime() * 1000000)
					+ " and vm='" + instance + "'" + " GROUP BY time(1m)";
			//System.out.println(cmd);
			JSONArray jArray = influxdbOP.selectQuery(cmd);
			double[] prediction=new double[30];
			if(jArray.size()>0){
				JSONArray jj=jArray.getJSONObject(0).getJSONArray("value");
				if(jj.size()>0){
					JSONObject object=jj.getJSONObject(0);
					if(object.has("mean")){
						double cur=object.getDouble("mean");
						for(int i=1;i<=30;i++){
							double pre=avg[i]-avg[i-1]+cur;
							if(pre<0)
								pre=0;
							prediction[i-1]=pre;
							cur=pre;
						}
					}
				}
			}
//			for(int i=0;i<prediction.length;i++)
//					System.out.println(prediction[i]);
			long[] timelist=new long[30];
			Date newdate=new Date(current_date.getTime());
			newdate.setSeconds(0);
			SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			for(int i=0;i<timelist.length;i++){
				timelist[i]=newdate.getTime();
				//System.out.println(df1.format(newdate));
				newdate.setTime(newdate.getTime()+60*1000);
			}
			_collector.emit(new Values(metric, instance, prediction, timelist));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		/*
		 * 定义元组格式 id: 虚拟机di name:虚拟机名 current_time:预测数据产生的时间
		 * preditcted_data:预测结果 actual_value:真实的历史数据 avg:历史数据平均值
		 */
		declarer.declare(new Fields("metric", "instance", "prediction", "timelist"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

//	public Memory_Bolt() {
//		// TODO Auto-generated constructor stub
//		InfluxdbOP influxdbOP = new InfluxdbOP();
//		String metric = "vmem_use";
//		Date current_date = new Date();
//		System.out.println(current_date.toGMTString());
//		String instance = "instance-0000005b";
//		List<JSONArray> history_data=new ArrayList<JSONArray>();
//		int period_num=7;
//		for (int j = 0; j < period_num; j++) {
//			Date start = new Date(current_date.getTime() - (long) (j + 1) * 24 * 60 * 60 * 1000 - 60 * 1000);
//			Date end = new Date(start.getTime() + 30 * 60 * 1000);
//			//System.out.println(current_date.toString());
//			String cmd = "select MEAN(value) from \"" + metric + "\" where time>"
//					+ String.valueOf(start.getTime() * 1000000) + " and time<" + String.valueOf(end.getTime() * 1000000)
//					+ " and vm='" + instance + "'" + " GROUP BY time(1m)";
//			//System.out.println(cmd);
//			JSONArray jArray = influxdbOP.selectQuery(cmd);
//			//System.out.println(jArray);
//			if (jArray.size() > 0)
//				history_data.add(jArray.getJSONObject(0).getJSONArray("value"));
//			else {
//				break;
//			}
//		}
//		System.out.println(history_data.toString());
//		double[] avg=new double[35];
//		int[] count=new int[35];
//		for(int i=0;i<history_data.size();i++){
//			JSONArray jsonArray=history_data.get(i);
//			for(int j=0;j<jsonArray.size();j++){
//				JSONObject object=jsonArray.getJSONObject(j);
//				if(object.has("mean")){
//					avg[j]+=object.getDouble("mean");
//					count[j]++;
//				}
//			}
//		}
//		for(int i=0;i<avg.length;i++){
//			if(count[i]!=0)
//				avg[i]/=count[i];
//		}
//		//for(int i=0;i<avg.length;i++)
//		//System.out.println(avg[i]);
//		//predict
//		Date start = new Date(current_date.getTime()  - 60 * 1000);
//		Date end = current_date;
//		//System.out.println(current_date.toString());
//		String cmd = "select MEAN(value) from \"" + metric + "\" where time>"
//				+ String.valueOf(start.getTime() * 1000000) + " and time<" + String.valueOf(end.getTime() * 1000000)
//				+ " and vm='" + instance + "'" + " GROUP BY time(1m)";
//		//System.out.println(cmd);
//		JSONArray jArray = influxdbOP.selectQuery(cmd);
//		double[] prediction=new double[30];
//		if(jArray.size()>0){
//			JSONArray jj=jArray.getJSONObject(0).getJSONArray("value");
//			if(jj.size()>0){
//				JSONObject object=jj.getJSONObject(0);
//				if(object.has("mean")){
//					double cur=object.getDouble("mean");
//					for(int i=1;i<=30;i++){
//						double pre=avg[i]-avg[i-1]+cur;
//						prediction[i-1]=pre;
//						cur=pre;
//					}
//				}
//			}
//		}
//		for(int i=0;i<prediction.length;i++)
//				System.out.println(prediction[i]);
//		long[] timelist=new long[30];
//		Date newdate=new Date(current_date.getTime());
//		newdate.setSeconds(0);
//		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
//		for(int i=0;i<timelist.length;i++){
//			timelist[i]=newdate.getTime()*1000000;
//			System.out.println(df1.format(newdate));
//			newdate.setTime(newdate.getTime()+60*1000);
//		}
//		
//		
//	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		predict_Bolt memory_Bolt = new predict_Bolt();
		String tt = "2016-12-09";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// df1.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			Date tDate = df.parse(tt);
			String dString = df1.format(tDate);
			System.out.println(dString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
