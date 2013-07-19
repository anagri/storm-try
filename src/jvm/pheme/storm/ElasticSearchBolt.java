package pheme.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SuppressWarnings("serial")
public class ElasticSearchBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchBolt.class);
    private OutputCollector collector;
    private Client client;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String elasticSearchHost = "localhost";
        Integer elasticSearchPort = 9300;

//        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "Eros").build();
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));
    }

    @Override
    public void execute(Tuple input) {
        String tweet = input.getString(0);
        String id = input.getString(1);
        String username = input.getString(2);
        boolean retweet = input.getBoolean(3);
        List<String> urls = (List<String>) input.getValue(4);
        Date date = (Date) input.getValue(5);
        String via = input.getString(6);

        try {
            IndexResponse response = client.prepareIndex("sprinklr", "twitter", id)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", username)
                            .field("tweet", tweet)
                            .field("retweet", retweet)
                            .field("postDate", date)
                            .field("urls", urls)
                            .field("via", via)
                            .endObject()
                    ).execute().actionGet();
            LOG.debug("Indexed Document[ " + id + "], Type[sprinklr], Index[twitter], Version [" + response.getVersion() + "]");
            collector.emit(new Values(tweet));
            collector.ack(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}