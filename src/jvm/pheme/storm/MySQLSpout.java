package pheme.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static java.lang.String.format;

public class MySQLSpout extends BaseRichSpout {
    private final String dbHost;
    private final String dbPort;
    private final String dbName;
    private final String dbUser;
    private final String dbPassword;
    private SpoutOutputCollector collector;

    private int lastId = 0;
    private MySQL mySQL;
    private Statement statement;
    private ResultSet resultSet;

    public MySQLSpout(String dbHost, String dbPort, String dbName, String dbUser, String dbPassword) {
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        mySQL = new MySQL(dbHost, dbPort, dbName, dbUser, dbPassword);
        statement = mySQL.statement();

        try {
            resultSet = fetchData();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        super.close();
        mySQL.close();
    }

    @Override
    public void nextTuple() {
        try {
            if (resultSet.next()) {
                String data = resultSet.getString("data");
                lastId = resultSet.getInt("id");
                collector.emit(new Values(data));
            } else {
                resultSet = fetchData();
                if (resultSet.next()) {
                    String data = resultSet.getString("data");
                    lastId = resultSet.getInt("id");
                    collector.emit(new Values(data));
                } else {
                    System.out.println("No data to process");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSet fetchData() throws SQLException {
        return statement.executeQuery(format("select id, userId, data, added from tweets where id > %d order by id limit 1000", lastId));
    }
}
