package pheme.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

class TweetParserBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String originalTweeet = input.getString(0);
        TweetParser parsedTweet = new TweetParser(originalTweeet);
        collector.emit(new Values(parsedTweet.tweet(), parsedTweet.id(), parsedTweet.username(), parsedTweet.isRetweet(), parsedTweet.urls(), parsedTweet.time(), parsedTweet.via()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "id", "username", "RT", "urls", "published", "via"));
    }
}
