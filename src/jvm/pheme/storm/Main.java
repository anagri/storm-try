package pheme.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class Main {

    private static MySQL mySQL;

    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet", new MySQLSpout(), 1);
        builder.setBolt("tweetParser", new TweetParserBolt(), 10).shuffleGrouping("tweet");
        builder.setBolt("elastic", new ElasticSearchBolt(), 10).shuffleGrouping("tweetParser");
        builder.setBolt("counter", new BaseRichBolt() {

            private Statement statement;
            private int count;
            private OutputCollector collector;

            @Override
            public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
                this.collector = collector;
                mySQL = new MySQL();
                statement = mySQL.statement();
            }

            @Override
            public void execute(Tuple input) {
                if(count % 1000 == 0)
                    try {
                        statement.executeQuery("update counter set counter=counter+1000");
                    } catch (SQLException e) { }
                count++;
                collector.ack(input);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        }, 10).shuffleGrouping("elastic");

        cluster.submitTopology("test", conf, builder.createTopology());

//        Utils.sleep(30 * 60 * 1000);
//        cluster.killTopology("test");
//        cluster.shutdown();
    }
}
