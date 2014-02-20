package storm;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import org.json.simple.parser.*;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
 // This is where you enter your Oauth info
 		final static String OAuthConsumerKey = "ScUJsmGtSbYe5PTebvPbqA";
 		final static String OAuthConsumerSecret = "Prp7ZiUfEY3KW4bWBIwtmlH9p9uFe5SYjgxJRiZpc";
 		 // This is where you enter your Access Token info
 		final static String AccessToken = "127215711-hY29KHrTjwRdYOvdAqI6Lxv082rdcQhNk5FxrHTR";
 		final static String AccessTokenSecret = "CuH3uC2kztiiYudnIESAFjpcAwtzhisgGTif4klOS48";
 		
 		

    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception e) {
            }

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
            
        };
        ConfigurationBuilder confb = new ConfigurationBuilder();
        confb.setOAuthConsumerKey(OAuthConsumerKey);
        confb.setOAuthConsumerSecret(OAuthConsumerSecret);
        confb.setOAuthAccessToken(AccessToken);
        confb.setOAuthAccessTokenSecret(AccessTokenSecret);
        TwitterStreamFactory fact = new TwitterStreamFactory(confb.build());
        _twitterStream = fact.getInstance();
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}