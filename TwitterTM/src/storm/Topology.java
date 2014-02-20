package storm;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


public class Topology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();        
		builder.setSpout("tweetSpout", new TwitterSpout(), 1);
		builder.setBolt("tweetBolt", new TwitterBolt(), 1).shuffleGrouping("tweetSpout");
		
		
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(300000);
		cluster.killTopology("test");
		cluster.shutdown();

	}

}
