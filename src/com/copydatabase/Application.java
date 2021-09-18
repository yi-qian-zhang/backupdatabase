package com.copydatabase;

import java.io.*;
import java.sql.SQLException;

/**
 * @author yi-qian-zhang
 */
public class Application {
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {

        DatabaseUser behaviour =new DatabaseUser();

        behaviour.outPrintln();

        behaviour.makeBackups();

        behaviour.copyDatabase();

        behaviour.statement.close();
        behaviour.connection.close();

    }
}
