package pheme.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static java.lang.String.format;

public class MySQLSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    private int lastId = 0;
    private MySQL mySQL;
    private Statement statement;
    private ResultSet resultSet;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        mySQL = new MySQL();
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
                    System.out.println("No data to process, trying after 1 minute.");
                    Thread.sleep(1000);
                    nextTuple();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSet fetchData() throws SQLException {
        return statement.executeQuery(format("select id, userId, data, added from CD_data where id > %d order by id limit 1000", lastId));
    }
}
