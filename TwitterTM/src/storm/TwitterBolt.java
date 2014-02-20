package storm;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.GeoLocation;
import twitter4j.MediaEntity;
import twitter4j.Status;
import twitter4j.URLEntity;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class TwitterBolt extends BaseRichBolt {

	OutputCollector _collector;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector= collector;
		
	}

	@Override
	public void execute(Tuple input) {
		
		Status status = (Status)input.getValue(0);
		String lang= status.getIsoLanguageCode();
		if(lang.equals("en")){
		String text= status.getText();
		System.out.println("TESTO: "+text);
		System.out.println("STATO: "+status.toString());
		_collector.emit(new Values(text));
		}
        _collector.ack(input);
		
		
	}
		
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
		
	}

}
