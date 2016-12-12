package storm.workload_prediction;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class DB_Bolt implements IRichBolt {

	OutputCollector _collector;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//"metric", "instance", "prediction", "timelist"
		
		String metric = input.getStringByField("metric");
		String instance = input.getStringByField("instance");
		double[] prediction = (double[]) input.getValueByField("prediction");
		long[] timelist = (long[]) input.getValueByField("timelist");
		InfluxdbOP influxdbOP=new InfluxdbOP(2);
		System.out.println("DB_Bolt.execute()"+timelist.length);
		if(influxdbOP.writedata(metric, instance, prediction, timelist))
			System.out.println("save to db "+metric+" "+instance);
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
