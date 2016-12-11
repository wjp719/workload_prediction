package storm.workload_prediction;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import net.sf.json.JSONArray;

public class VM_Spout extends BaseRichSpout {
	//public class VM_Spout  {
	SpoutOutputCollector _collector;
	static String tenant_id = "56fc364c204043b98a438122568fbf14";
	int pre_time;
	int cur_time;

	/* 获取当前时间 */
	public static int get_current_time() {
		int current = 0;
		Date current_date = new Date();
		int hour = current_date.getHours();
		int minute = current_date.getMinutes();
		int add = 0;
		if (minute >= 30) {
			add = 1;
		}
		current = 2 * hour + add;
		return current;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		pre_time = -1;
	}

	/* 查询数据 此节点负责查询内存数据 */
	public void nextTuple() {
		// TODO Auto-generated method stub

		// cur_time = get_current_time();
		// if (pre_time != cur_time) {
		// pre_time = cur_time;
		Property property = new Property();
		String vm_metrics = property.getProperty("vm_metrics");
		String[] meticarr = vm_metrics.split(",");
		String token = null;
		OpenStackOP op = new OpenStackOP();
		List<String> run_vm_list = null;
		try {
			token = op.getToken();
			run_vm_list = op.getVmList(token);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("run_vm_list"+run_vm_list.size());
        
		HashMap<String, Integer> vMap = new HashMap<String, Integer>();
		for (int j = 0; j < run_vm_list.size(); j++) {
			vMap.put(run_vm_list.get(j), 1);
			//System.out.println("VM_Spout.nextTuple()");
		}
		InfluxdbOP influxdbOP = new InfluxdbOP();
		for (int i = 0; i < meticarr.length; i++) {
			String metic = meticarr[i];
			JSONArray jArray = influxdbOP.getVMseries(metic);
			System.out.println(metic+" "+jArray.size());
			for (int j = 0; j < jArray.size(); j++) {
				String instance = jArray.getString(j);
				if (vMap.get(instance)!=null) {
					_collector.emit(new Values(metic, instance));
					//System.out.println("VM_Spout.nextTuple()" + metic + " " + instance);
				}
			}
		}
		try {
			Thread.sleep(1000 * 10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// }

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		VM_Spout v=new VM_Spout();
		v.nextTuple();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("metrics", "instance"));// VM信息以及查询所需的token
	}

}
