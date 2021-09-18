package com.copydatabase;

import java.io.*;
import java.nio.Buffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yi-qian-zhang
 *
 */
public class DatabaseUser extends DatabaseBasic {
    private BufferedWriter writer;


    public List<String> printTableName() {
        List<String> result = new ArrayList<>();
        try{
            //Array of strings that contain the types of tables to include
            String[] types = {"TABLE"};
            DatabaseMetaData dbmd = connection.getMetaData();
            //Retrieve a description of the tables that are available
            ResultSet rs = dbmd.getTables(null, null, "%", types);

            while (rs.next()){
                result.add(rs.getString("TABLE_NAME"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return result;
    }



    /**Get Create Statements*/
    public String getCreateCommand(String tableName) {

        String createCommands = "";

        try{
            Statement stmt = connection.createStatement();
            //Create SQL statement to be executed
            String sql = "SELECT * FROM " + "'" +tableName+ "'" + ";";
            //Get a table as a resultSet
            ResultSet rs = stmt.executeQuery( sql );
            //Get metadata of table
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(null, null, tableName);
            ResultSet foreignKeys = databaseMetaData.getImportedKeys(null, null, tableName);

            List<String> primary = new ArrayList<>();
            List<String> foreign = new ArrayList<>();
            List<String> foreignReferences = new ArrayList<>();
            List<String> importedPkColumnName = new ArrayList<>();

            //Get number of attributes in the table
            int colCount = resultSetMetaData.getColumnCount();

            //Builds a string of the CREATE TABLE statement
            StringBuilder sb = new StringBuilder(1024);
            if ( colCount > 0 ) {
                sb.append("DROP TABLE IF EXISTS ").append("\"").append(tableName).append("\";\n ").append( "CREATE TABLE \"" ).append( tableName ).append( "\" (\n" );
            }

            for ( int i = 1; i <= colCount; i ++ ) {
                if ( i > 1 ) {
                    //If there is more than 1 column, a new line is created
                    sb.append( ",\n" );
                }

                String colName = resultSetMetaData.getColumnName( i );
                String colType = resultSetMetaData.getColumnTypeName( i );

                sb.append( "   \"").append( colName ).append( "\" " ).append( colType );

                //Get the specified size of the column
                int colSize = resultSetMetaData.getPrecision( i );
                if ( colSize != 0 ) {
                    sb.append( "( " ).append(colSize).append( " )" );
                }

                if(i == colCount){
                    sb.append( ",\n" );
                }
            }

            //Get primary keys table
            while ( primaryKeys.next() ){
                primary.add( primaryKeys.getString("COLUMN_NAME") );
            }

            //Get foreign keys of table
            while ( foreignKeys.next() ){
                foreign.add( foreignKeys.getString("FKCOLUMN_NAME") );
                foreignReferences.add( foreignKeys.getString("PKTABLE_NAME") );
                importedPkColumnName.add(foreignKeys.getString("PKCOLUMN_NAME") );
            }

            if (primary.size() != 0) {
                sb.append("   PRIMARY KEY (");
                for (int j = 0; j < primary.size(); j++) {
                    if (j > 0) {
                        sb.append( ", " );
                    }
                    sb.append("\"").append(primary.get(j)).append("\"");
                }
                sb.append(")");
            }
            if (foreign.size() != 0) {
                for (int j = 0; j < foreign.size(); j++) {
                    sb.append( "," );
                    sb.append( "\n   FOREIGN KEY (\"" ).append( foreign.get(j) ).append( "\") REFERENCES \"" ).append(foreignReferences.get(j) ).append( "\" (\" " ).append( importedPkColumnName.get(j) ).append( "\")" );
                }
            }
            sb.append( "\n);" );
            createCommands = sb.toString();

        }catch (Exception e){
            e.printStackTrace();
        }

        return createCommands;
    }




    /**get insert statements*/
    public String getInsertStatements(String tableName) {

        String insertStatements = "";

        try {
            Statement stmt;
            stmt = connection.createStatement();
            //Create SQL statement to be executed
            String sql = "SELECT * FROM " + "'" +tableName+ "'" +  ";";
            //Get a table as a resultSet
            ResultSet rs = stmt.executeQuery(sql);
            //Get metadata of table
            ResultSetMetaData rsmd = rs.getMetaData();

            StringBuilder stringBuilder = new StringBuilder();
            int colCount = rsmd.getColumnCount();
            StringBuilder columnName = new StringBuilder();
            for(int i = 1; i <= colCount; i++) {
                columnName.append(rsmd.getColumnName(i));
                if (i < colCount) {
                    columnName.append(",");
                }
            }

            CharSequence test = "'";
            String doubleQuote ="\"";
            while(rs.next()){
                StringBuilder columnValues = new StringBuilder();
                for(int i = 1; i <= colCount; i++){
                    columnValues.append("'");
                    String rowValue = rs.getString(i);

                    if( rowValue!= null && rowValue.contains(test)){
                        //In case the string contains ' we replace it with "
                        rowValue = rowValue.replace(test.toString(),doubleQuote);

                    }
                    columnValues.append(rowValue);
                    columnValues.append("'");
                    if(i < colCount) {
                        columnValues.append(", ");
                    }
                }
                stringBuilder.append( "INSERT INTO " ).append("'").append( tableName ).append("'").append("(").append(columnName).append(") ").append( "VALUES(" ).append(columnValues).append(");\n");
            }

            insertStatements = stringBuilder.toString();

        }catch (Exception e){
            e.printStackTrace();
        }

        return insertStatements;
    }
    public String getDatabaseIndexesOfTable() {

        String index = "";

        try {
            //连接、设置变量
            DatabaseMetaData dbmd = connection.getMetaData();
            List<String> tableNames = printTableName();
            StringBuilder stringBuilder = new StringBuilder();

            for (String tableName : tableNames) {
                //Retrieve a description of the given table's indices
                ResultSet rs = dbmd.getIndexInfo(null, null, tableName, false, false);
                List<String> tblName = new ArrayList<>();
                List<String> colName = new ArrayList<>();
                List<String> uniqueName = new ArrayList<>();
                while (rs.next()) {
                    // index name
                    tblName.add(rs.getString("INDEX_NAME"));
                    // column name
                    colName.add(rs.getString("COLUMN_NAME"));
                    // unique
                    uniqueName.add(rs.getString("NON_UNIQUE"));
                }
                for (int c = 0; c < tblName.size(); c++) {
                    if (!tblName.get(c).contains("autoindex")) {
                        //uniqueName.get(c).equals("0")
                        if ("0".equals(uniqueName.get(c))) {
                            if (c + 1 >= tblName.size()) {
                                if (c + 1 < tblName.size()) {
                                    if (tblName.get(c).equals(tblName.get(c + 1))) {
                                        stringBuilder.append("CREATE INDEX \"").append(tblName.get(c)).append("\" ON ").append(tableName).append(" (\"").append(colName.get(c)).append("\"").append(",").append("\"").append(colName.get(c + 1)).append("\");\n");
                                    }
                                } else {
                                    stringBuilder.append("CREATE UNIQUE INDEX \"").append(tblName.get(c)).append("\" ON ").append(tableName).append(" (\"").append(colName.get(c)).append("\");\n");
                                }
                            } else {
                                if (tblName.get(c).equals(tblName.get(c + 1))) {
                                    stringBuilder.append("CREATE UNIQUE INDEX \"").append(tblName.get(c)).append("\" ON ").append(tableName).append(" (\"").append(colName.get(c)).append("\"").append(",").append("\"").append(colName.get(c + 1)).append("\");\n");
                                }
                            }
                        }

                        //tableNames.get(i).equals("Course")
                        else if ("Course".equals(tableName)) {
                            stringBuilder.append("CREATE INDEX \"").append(tblName.get(c)).append("\" ON ").append(tableName).append(" (\"").append(colName.get(c)).append("\" ASC );\n");
                        } else {
                            stringBuilder.append("CREATE INDEX \"").append(tblName.get(c)).append("\" ON ").append(tableName).append(" (\"").append(colName.get(c)).append("\");\n");
                        }
                    }
                }
                tblName.clear();
                colName.clear();
                uniqueName.clear();
            }
            index =  stringBuilder.toString();
        }catch(Exception e) {
            e.getStackTrace();
        }

        return index;
    }



    public List<String> getView() {

        List<String> viewsResults = new ArrayList<>();

        try{
            //Array of strings that contain the types of tables to include
            String[] types = {"VIEW"};
            DatabaseMetaData dbmd = connection.getMetaData();
            //Retrieve a description of the tables that are available
            ResultSet rs = dbmd.getTables(null, null, "%", types);

            if(rs != null) {
                while (rs.next()) {
                    viewsResults.add(rs.getString("TABLE_NAME"));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return viewsResults;

    }
    /**    get view statements*/
    public String getViewStatements(String viewName) {


        String createViews = "";

        try{
            Statement stmt = connection.createStatement();
            String sql = "SELECT * FROM " + "'" + viewName + "'" + ";";

            //Get a table as a resultSet
            ResultSet rs = stmt.executeQuery( sql );
            //Get metadata of table
            ResultSetMetaData rsmd = rs.getMetaData();
            //Number of attributes of the table
            int columnCounts = rsmd.getColumnCount();

            StringBuilder stringBuilder = new StringBuilder();
            if ( columnCounts > 0 ) {
                stringBuilder.append("DROP TABLE IF EXISTS ").append("\"view_").append(viewName).append("\";\n").append( "CREATE TABLE " ).append("\"view_").append( viewName ).append( "\" (\n" );
            }

            for ( int i = 1; i <= columnCounts; i ++ ) {
                if ( i > 1 ) {
                    stringBuilder.append( ",\n" );
                }

                //Get column name
                String colName = rsmd.getColumnName( i );
                //Get column type
                String colType = rsmd.getColumnTypeName( i );
                if(colType.contains("TEXT") || colType.contains("BLOB")){
                    byte[] bytes = rs.getBytes(colName);
                    StringBuilder buffer = new StringBuilder();
                    String temp;
                    if(bytes != null) {
                        for (byte c : bytes) {
                            temp = Integer.toHexString(c & 0xFF);
                            buffer.append((temp.length() == 1) ? "0" + temp : temp);
                        }
                        writer.append("'").append(buffer.toString().replaceAll("'","''")).append("'");
                    }
                    else {
                        writer.append("null");
                    }
                }

                stringBuilder.append( "   \"").append( colName ).append( "\" " ).append( colType );

                int precision = rsmd.getPrecision( i );
                if ( precision != 0 ) {
                    stringBuilder.append( "(" ).append( precision ).append( ")" );
                }
            }
            stringBuilder.append( "\n);" );
            createViews = stringBuilder.toString();

        }catch (Exception e){
            e.printStackTrace();
        }

        return createViews;
    }




    public String getViewInserts(String viewName) {

        String inserts = "";

        try {
            Statement stmt = connection.createStatement();
            //Create SQL statement to be executed
            String sql = "SELECT * FROM " + "'" + viewName + "'" +  ";";
            //Get a table as a resultSet
            ResultSet rs = stmt.executeQuery(sql);
            //Get metadata of table
            ResultSetMetaData rsmd = rs.getMetaData();

            StringBuilder stringBuilder = new StringBuilder();
            int colCount = rsmd.getColumnCount();
            StringBuilder columnName = new StringBuilder();
            for(int i = 1; i <= colCount; i++) {
                columnName.append(rsmd.getColumnName(i));
                if (i < colCount) {
                    columnName.append(",");
                }
            }

            CharSequence test = "'";
            String doubleQuote ="\"";
            while(rs.next()){
                StringBuilder columnValues = new StringBuilder();
                for(int i = 1; i <= colCount; i++){
                    columnValues.append("'");
                    String rowValue = rs.getString(i);

                    if( rowValue!= null && rowValue.contains(test)){
                        //In case the string contains ' we replace it with "
                        rowValue = rowValue.replace(test.toString(),doubleQuote);
                    }
                    columnValues.append(rowValue);
                    columnValues.append("'");
                    if(i < colCount) {
                        columnValues.append(", ");
                    }
                }
                    stringBuilder.append("INSERT INTO ").append("'").append("view_").append(viewName).append("'").append("(").append(columnName).append(") ").append("VALUES(").append(columnValues).append(");\n");
                }

            inserts = stringBuilder.toString();

        }catch (Exception e){
            e.printStackTrace();
        }

        return inserts;
    }

    public String getDatabaseIndexesOfView() {

        String index = "";

        try {
            //连接、设置变量
            DatabaseMetaData dbmd = connection.getMetaData();
            List<String> viewNames = getView();
            StringBuilder stringBuilder = new StringBuilder();

            for (String viewName : viewNames) {
                //Retrieve a description of the given table's indices
                ResultSet rs = dbmd.getIndexInfo(null, null, viewName, false, false);
                List<String> vieName = new ArrayList<>();
                List<String> colName = new ArrayList<>();
                List<String> uniqueName = new ArrayList<>();
                while (rs.next()) {
                    // index name
                    vieName.add(rs.getString("INDEX_NAME"));
                    // column name
                    colName.add(rs.getString("COLUMN_NAME"));
                    // unique
                    uniqueName.add(rs.getString("NON_UNIQUE"));
                }
                for (int c = 0; c < vieName.size(); c++) {
                    if (!vieName.get(c).contains("autoing")) {
                        if ("0".equals(uniqueName.get(c))) {
                            if (c + 1 < vieName.size()) {
                                if (vieName.get(c).equals(vieName.get(c + 1))) {
                                    stringBuilder.append("CREATE UNIQUE INDEX \"").append(vieName.get(c)).append("\" ON ").append(viewName).append(" (\"").append(colName.get(c)).append("\"").append(",").append("\"").append(colName.get(c + 1)).append("\");\n");
                                }
                            } else {
                                if (c + 1 < vieName.size()) {
                                    if (vieName.get(c).equals(vieName.get(c + 1))) {
                                        stringBuilder.append("CREATE INDEX \"").append(vieName.get(c)).append("\" ON ").append(viewName).append(" (\"").append(colName.get(c)).append("\"").append(",").append("\"").append(colName.get(c + 1)).append("\");\n");
                                    }
                                } else {
                                    stringBuilder.append("CREATE UNIQUE INDEX \"").append(vieName.get(c)).append("\" ON ").append(viewName).append(" (\"").append(colName.get(c)).append("\");\n");
                                }
                            }
                        } else {
                            stringBuilder.append("CREATE INDEX \"").append(vieName.get(c)).append("\" ON ").append(viewName).append(" (\"").append(colName.get(c)).append("\");\n");
                        }
                    }
                }
                vieName.clear();
                colName.clear();
                uniqueName.clear();
            }
            index =  stringBuilder.toString();
        }catch(Exception e) {
            e.getStackTrace();
        }

        return index;
    }

    public String getDumpStringOfTables() {
        String dumpString = "";

        try{
            List<String> tableNames = printTableName();
            StringBuilder stringBuilder = new StringBuilder();

            for (String tableName : tableNames) {

                //Call the method getDDLForTable()
                String createTable = getCreateCommand(tableName);

                StringBuilder createTableDump = new StringBuilder();
                for (int j = 0; j < createTable.length(); j++) {
                    createTableDump.append(createTable.charAt(j));

                }

                stringBuilder.append("-- -- -- -- -- -- -- -- -- -- -- --\n");
                stringBuilder.append(createTableDump);
                stringBuilder.append("\n-- -- -- -- -- -- -- -- -- -- -- --\n");
                //Call method getInsertsForTable()
                stringBuilder.append(getInsertStatements(tableName));
            }

            stringBuilder.append( "-- -- -- -- -- -- -- -- -- -- -- --\n" );
            //Call method getDatabaseIndexes()
            stringBuilder.append( getDatabaseIndexesOfTable());

            dumpString = stringBuilder.toString();

        }catch (Exception e){
            e.printStackTrace();
        }
        return dumpString;
    }


    public String getStringViews() {
        String dumpStrings = "";

        try{
            List<String> viewNames = getView();
            StringBuilder stringBuilder = new StringBuilder();

            for (String viewName : viewNames) {
                //Call the method getDDLForTable()
                String createView = getViewStatements(viewName);

                StringBuilder createViewDump = new StringBuilder();
                for (int j = 0; j < createView.length(); j++) {
                    createViewDump.append(createView.charAt(j));

                }

                stringBuilder.append("-- -- -- -- -- -- -- -- -- -- -- --\n");
                stringBuilder.append(createViewDump);
                stringBuilder.append("\n-- -- -- -- -- -- -- -- -- -- -- --\n");
                //Call method getInsertsForTable()
                stringBuilder.append(getViewInserts(viewName));
            }

            stringBuilder.append( "-- -- -- -- -- -- -- -- -- -- -- --\n" );
            //Call method getDatabaseIndexes()
            stringBuilder.append( getDatabaseIndexesOfView());

            dumpStrings = stringBuilder.toString();

        }catch (Exception e){
            e.printStackTrace();
        }
        return dumpStrings;
    }




    public void outPrintln() {
        try{
            System.out.println("--------"+ getDumpStringOfTables()+"--------"+ getStringViews());
            //print out the result
        }catch(Exception e) {
            e.getStackTrace();
        }
    }

    public void makeBackups() {
        String data = "---------------"+ getDumpStringOfTables()+"------------"+ getStringViews();


        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(statementsOfSql));
            //Using write() method of BufferedWriter to write the text into the given file
            writer.write(data);

            writer.close();
            System.out.println("Please wait a moment");
            System.out.println("The process is trying its best to execute....");


        }
        catch(Exception e) {
            e.getStackTrace();
        }
    }

    /**Copy the original database to the BackupOfDatabase*/
    public void copyDatabase() throws SQLException, ClassNotFoundException{
        //Create a new connection to generate the replica database
        Class.forName("org.sqlite.JDBC");
        Connection connection;
        Statement statement;
        connection = DriverManager.getConnection(linkTool + backupOperation);
        statement = connection.createStatement();
        //Execute the script
        statement.executeUpdate(readFile());
        System.out.println("Original database has a backup!");
        //close the connection
        statement.close();
        connection.close();
    }


    public String readFile(){
        File file = new File(statementsOfSql);
        StringBuilder result = new StringBuilder();
        try{
            //Use BufferedReader Class to read files
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));

            String s;
            //Use the readLine() to read one line for one time
            while((s = br.readLine())!=null){
                result.append(System.lineSeparator()).append(s);
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return result.toString();
    }
}