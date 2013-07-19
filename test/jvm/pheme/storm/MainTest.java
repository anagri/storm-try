package pheme.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.junit.Test;

public class MainTest {
    @Test
    public void testMain() throws Exception {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet", new MySQLSpout(), 1);
        builder.setBolt("tweetParser", new TweetParserBolt(), 10).shuffleGrouping("tweet");
        builder.setBolt("elastic", new ElasticSearchBolt()).shuffleGrouping("tweetParser");

        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(3 * 60 * 1000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

}
