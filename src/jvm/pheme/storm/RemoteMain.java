package pheme.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class RemoteMain {
    public static void main(String[] args) {
        int tweetParserCount = Integer.parseInt(args[0]);
        int elasticCount = Integer.parseInt(args[1]);
        String elasticHost = args[2];
        int elasticPort = Integer.parseInt(args[3]);
        String indexName = args[4];
        String indexType = args[5];
        String dbHost = args[6];
        String dbPort = args[7];
        String dbName = args[8];
        String dbUser = args[9];
        String dbPassword = args[10];

        try {
            Config conf = new Config();
            conf.setDebug(false);
            conf.setNumWorkers(20);
            conf.setMaxSpoutPending(5000);

            StormSubmitter.submitTopology("sprinklr-storm-spike", conf,
                    new MySQLElasticTopology()
                            .get(tweetParserCount, elasticCount,
                                    elasticHost, elasticPort, indexName, indexType,
                                    dbHost, dbPort, dbName, dbUser, dbPassword));
        } catch (AlreadyAliveException e) {
            throw new RuntimeException(e);
        } catch (InvalidTopologyException e) {
            throw new RuntimeException(e);
        }
    }
}
