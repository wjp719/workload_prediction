package storm.workload_prediction;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class topology {

	
	public static void main(String[] args) throws Exception {
		
	TopologyBuilder builder = new TopologyBuilder();
	//虚拟机

	builder.setSpout("VM_Spout", new VM_Spout(), 1);
	
	builder.setBolt("M_Bolt", new Memory_Bolt(),5).shuffleGrouping("VM_Spout");
	builder.setBolt("D_Bolt", new Disk_Bolt(),5).shuffleGrouping("VM_Spout");
	builder.setBolt("C_Bolt", new CPU_Bolt(),5).shuffleGrouping("VM_Spout");
	
	builder.setBolt("DB_Bolt_M", new DB_Bolt(),5).shuffleGrouping("M_Bolt");
	builder.setBolt("DB_Bolt_D", new DB_Bolt(),5).shuffleGrouping("D_Bolt");
	builder.setBolt("DB_Bolt_C", new DB_Bolt(),5).shuffleGrouping("C_Bolt");
	
	//物理机
	builder.setSpout("PM_Spout", new PM_Spout(), 1);
	
	builder.setBolt("PM_M_Bolt", new PM_Memory_Bolt(),5).shuffleGrouping("PM_Spout");
	builder.setBolt("PM_D_Bolt", new PM_Disk_Bolt(),5).shuffleGrouping("PM_Spout");
	builder.setBolt("PM_C_Bolt", new PM_CPU_Bolt(),5).shuffleGrouping("PM_Spout");
	
	builder.setBolt("DB_Bolt_PM_M", new DB_Bolt(),5).shuffleGrouping("PM_M_Bolt");
	builder.setBolt("DB_Bolt_PM_D", new DB_Bolt(),5).shuffleGrouping("PM_D_Bolt");
	builder.setBolt("DB_Bolt_PM_C", new DB_Bolt(),5).shuffleGrouping("PM_C_Bolt");

	 Config conf = new Config();
     conf.setDebug(false);
     
     if (args != null && args.length > 0) {
         /*设置该topology在storm集群中要抢占的资源slot数，一个slot对应这supervisor节点上的以个worker进程
        如果你分配的spot数超过了你的物理节点所拥有的worker数目的话，有可能提交不成功，加入你的集群上面已经有了
        一些topology而现在还剩下2个worker资源，如果你在代码里分配4个给你的topology的话，那么这个topology可以提交
        但是提交以后你会发现并没有运行。 而当你kill掉一些topology后释放了一些slot后你的这个topology就会恢复正常运行。
       */
        conf.setNumWorkers(8);

        StormSubmitter.submitTopology(args[0], conf,
                builder.createTopology());
    } else {
        conf.setMaxTaskParallelism(1);
       
        //指定为本地模式运行
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Workload_Prediction", conf, builder.createTopology());


      //  cluster.shutdown();
    }
	
	}
}
