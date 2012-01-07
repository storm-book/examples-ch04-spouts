package twitter.streaming;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new ApiStreamingSpout(),2);
		builder.setBolt("hashtag-sumarizer", new TwitterSumarizeHashtags()).
			shuffleGrouping("tweets-collector"); 
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.put("track", args[0]);
		conf.put("user", args[1]);
		conf.put("password", args[2]);
		
		cluster.submitTopology("twitter-test", conf, builder.createTopology());
	}
}
