package pheme.storm;

import java.io.Serializable;
import java.sql.*;

import static java.lang.String.format;

public class MySQL implements Serializable {
    private Connection connection;

    public MySQL(String dbHost, String dbPort, String dbName, String dbUser, String dbPassword) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(format("jdbc:mysql://%s:%s/%s?user=%s&password=%s", dbHost, dbPort, dbName, dbUser, dbPassword));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Statement statement() {
        try {
            return connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace(System.err);
        }
    }
}
