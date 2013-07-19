package pheme.storm;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class MySQLElasticTopology {
    public StormTopology get(int tweetParserCount, int elasticCount, String elasticHost,
                             int elasticPort, String indexName, String type, String dbHost, String dbPort, String dbName, String dbUser, String dbPassword) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet", new MySQLSpout(dbHost, dbPort, dbName, dbUser, dbPassword), 1);
        builder.setBolt("tweetParser", new TweetParserBolt(), tweetParserCount).shuffleGrouping("tweet");
        builder.setBolt("elastic", new ElasticSearchBolt(elasticHost, elasticPort, indexName, type), elasticCount).shuffleGrouping("tweetParser");
        return builder.createTopology();
    }
}
