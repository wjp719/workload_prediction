package storm.workload_prediction;

import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class DB_Bolt implements IRichBolt {

	OutputCollector _collector;
	DBCollection coll;
	String[] time = { "00:00:00", "00:30:00", "01:00:00", "01:30:00", "02:00:00", "02:30:00", "03:00:00", "03:30:00",
			"04:00:00", "04:30:00", "05:00:00", "05:30:00", "06:00:00", "06:30:00", "07:00:00", "07:30:00", "08:00:00",
			"08:30:00", "09:00:00", "09:30:00", "10:00:00", "10:30:00", "11:00:00", "11:30:00", "12:00:00", "12:30:00",
			"13:00:00", "13:30:00", "14:00:00", "14:30:00", "15:00:00", "15:30:00", "16:00:00", "16:30:00", "17:00:00",
			"17:30:00", "18:00:00", "18:30:00", "19:00:00", "19:30:00", "20:00:00", "20:30:00", "21:00:00", "21:30:00",
			"22:00:00", "22:30:00", "23:00:00", "23:30:00" };// 时间00:00：00-23:30:00，间隔30分钟

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		// ArrayList<VM> VM_List = (ArrayList<VM>)
		// input.getValueByField("VM_list");
		String id = (String) input.getValueByField("id");
		String name = (String) input.getValueByField("name");
		String meter = (String) input.getValueByField("meter");
		int current_time = (Integer) input.getValueByField("current_time");
		double[] predicted_value = (double[]) input.getValueByField("preditcted_data");
		double[] actual_value = (double[]) input.getValueByField("actual_value");
		double[] avg = (double[]) input.getValueByField("avg");
		System.out.println("current_time:" + current_time);

		// System.out.println(name+" "+meter);
		/* 连接数据库 */
		MongoCredential credential = MongoCredential.createCredential("root", "admin", "tsinghuamcloud".toCharArray());
		ServerAddress serverAddress = null;
		try {
			serverAddress = new ServerAddress("166.111.143.220", 27018);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MongoClient mongoClient = new MongoClient(serverAddress, Arrays.asList(credential));
		DB db = mongoClient.getDB("admin");
		DBCollection coll = db.getCollection("v_m");

		/* 初始化数据对象 */

		/*
		 * 查询历史数据 有之前的记录则进行更新 无记录则插入新纪录
		 */
		BasicDBObject query = new BasicDBObject();
		query.put("id", id);
		query.put("meter", meter);
		DBCursor cursor = coll.find(query);
		coll.remove(query);// 删除旧记录

		// 插入新纪录
		DBObject data = new BasicDBObject();
		/* 获取虚拟机，指标信息 */
		data.put("id", id);
		data.put("name", name);
		data.put("meter", meter);
		data.put("current_time", current_time);
		/* 获取数据信息 */
		ArrayList<DBObject> data_list = new ArrayList<DBObject>();
		Date current_date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
		for (int j = 0; j < 48; j++) {

			DBObject d = new BasicDBObject();
			d.put("date", df.format(current_date) + " " + time[j]);// 每个数据对应的时间);
			d.put("predicted_value", predicted_value[j]);
			d.put("actual_value", actual_value[j]);
			d.put("avg", avg[j]);
			data_list.add(d);
		}
		data.put("data", data_list);
		/* 将data存入数据库 */

		coll.insert(data);

		/* 完成一次更新 */
		System.out.println(df.format(current_date) + " " + meter + " update done");

	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
