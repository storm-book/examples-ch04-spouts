package banktransactions;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TransactionsSpouts implements IRichSpout{

	private static final Integer MAX_FAILS = 2;
	Map<Integer,String> messages;
	Map<Integer,Integer> failCounterMessages;
	Map<Integer,String> toSend;
	private SpoutOutputCollector collector;  
	
	static Logger LOG = Logger.getLogger(TransactionsSpouts.class);
	
	public boolean isDistributed() {
		return false;
	}

	public void ack(Object msgId) {
		messages.remove(msgId);
		LOG.info("Message fully processed ["+msgId+"]");
	}

	public void close() {
		
	}

	public void fail(Object msgId) {
		if(!failCounterMessages.containsKey(msgId))
			throw new RuntimeException("Error, transaction id not found ["+msgId+"]");
		Integer transactionId = (Integer) msgId;
		
		//Get the transactions fail
		Integer fails = failCounterMessages.get(transactionId) + 1;
		if(fails >= MAX_FAILS){
			//If exceeds the max fails will go down the topology
			throw new RuntimeException("Error, transaction id ["+transactionId+"] has had many errors ["+fails+"]");
		}
		//If not exceeds the max fails we save the new fails quantity and re-send the message 
		failCounterMessages.put(transactionId, fails);
		toSend.put(transactionId,messages.get(transactionId));
		LOG.info("Re-sending message ["+msgId+"]");
	}

	public void nextTuple() {
		if(!toSend.isEmpty()){
			for(Map.Entry<Integer, String> transactionEntry : toSend.entrySet()){
				Integer transactionId = transactionEntry.getKey();
				String transactionMessage = transactionEntry.getValue();
				collector.emit(new Values(transactionMessage),transactionId);
			}
			/*
			 * The nextTuple, ack and fail methods run in the same loop, so
			 * we can considerate the clear method atomic
			 */
			toSend.clear();
		}
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {}
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		Random random = new Random();
		messages = new HashMap<Integer, String>();
		toSend = new HashMap<Integer, String>();
		failCounterMessages = new HashMap<Integer, Integer>();
		for(int i = 0; i< 100; i++){
			messages.put(i, "transaction_"+random.nextInt());
			failCounterMessages.put(i, 0);
		}
		toSend.putAll(messages);
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transactionMessage"));
	}

}
