package twitter.streaming;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TwitterSumarizeHashtags implements IBasicBolt{

	Map<String, Integer> hashtags = new HashMap<String, Integer>();
	
	@Override
	public void cleanup() {
		
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		JSONObject json = (JSONObject)input.getValueByField("tweet");
		if(json.containsKey("entities")){
			JSONObject entities = (JSONObject) json.get("entities");
				if(entities.containsKey("hashtags")){
					for(Object hashObj : (JSONArray)entities.get("hashtags")){
						JSONObject hashJson = (JSONObject)hashObj;
						String hash = hashJson.get("text").toString().toLowerCase();
						if(!hashtags.containsKey(hash)){
							hashtags.put(hash, 1);
						}else{
							Integer last = hashtags.get(hash);
							hashtags.put(hash, last + 1);
						}
					}
			}
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		TimerTask task = new TimerTask() {
			
			@Override
			public void run() {
				Map<String, Integer> oldMap = new HashMap<String, Integer>(hashtags);
				hashtags.clear();
				for(Map.Entry<String, Integer> entry : oldMap.entrySet()){
					System.out.println(entry.getKey()+": "+entry.getValue());
				}
			}
			
		};
		Timer t = new Timer();
		t.scheduleAtFixedRate(task, 60000, 60000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
