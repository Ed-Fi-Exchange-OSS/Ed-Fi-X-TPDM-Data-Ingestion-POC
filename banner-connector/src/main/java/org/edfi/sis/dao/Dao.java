package org.edfi.sis.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class Dao {
    private final static Logger logger = LoggerFactory.getLogger(Dao.class);

    @Value( "${database.url}" )
    String url;
    @Value( "${database.driver}" )
    String driver;
    @Value( "${database.username}" )
    String username;
    @Value( "${database.password}" )
    String password;

    Connection connection = null;


    public void getRemoteConnection() throws ClassNotFoundException, SQLException {
        Class.forName(getDriver());
        setConnection(DriverManager.getConnection(getUrl(), getUsername(), getPassword()));
    }

    public void closeRemoteConnection() throws SQLException {
        if (getConnection()!=null) {
            getConnection().close();
        }
    }

    public List<List<String>> makeSqlCall(String sql) {
        Statement st = null;
        List<List<String>> result = new ArrayList<>();

        try {
            st = connection.createStatement();
            ResultSet resultSet = st.executeQuery(sql);

            int columnCount = resultSet.getMetaData().getColumnCount();

            //Add the column headers
            List<String> headerRow = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                headerRow.add(resultSet.getMetaData().getColumnName(i));
            }
            result.add(headerRow);

            while (resultSet.next()) {
                List<String> dataRow = new ArrayList<>();

                for (int i = 1; i <= columnCount; i++) {
                    if (resultSet.getObject(i) != null) {
                        String data = resultSet.getObject(i).toString();
                        dataRow.add(data);
                    } else {
                        String data = "[null]";
                        dataRow.add(data);
                    }
                }
                result.add(dataRow);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return result;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
