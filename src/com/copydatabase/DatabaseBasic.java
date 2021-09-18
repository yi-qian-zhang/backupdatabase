package com.copydatabase;

import java.io.*;
import java.sql.*;
import java.util.Scanner;

/**
 * @author yi-qian-zhang
 */
public class DatabaseBasic {
	protected String linkTool = "jdbc:sqlite:";
	protected String originalDatabase = null;
	protected String backupOperation = null;
	protected String statementsOfSql = null;

	protected Connection connection = null;
	protected Statement statement = null;

	/**
	 * Name of database driver
	 *
	 */
	private static final String JDBC_DRIVER		= "org.sqlite.JDBC";

	/**
	 * Filesystem path to database
	 *
	 */
	public  String nameOfDatabase;

	/**
	 * Set to true to enable debug messages
	 */
	boolean mistake = false;
	/**
	 * Outputs a stacktrace for debugging and exits
	 * <p>
	 * To be called following an {@link Exception}
	 *
	 * @param message	informational String to display
	 * @param e		the Exception
	 */
	public void notice( String message, Exception e ) {
		System.out.println( message + " : " + e );
		e.printStackTrace ( );
		System.exit( 0 );
	}
	
	/**
	 * Establish JDBC connection with database
	 * <p>
	 * Autocommit is turned off delaying updates
	 * until commit( ) is called
	 */
	private void getConnection( ) {
		try {
			connection = DriverManager.getConnection(
					  linkTool
					+ nameOfDatabase);
			/*
			 * Turn off AutoCommit:
			 * delay updates until commit( ) called
			 */
			connection.setAutoCommit(false);
		}
		catch ( SQLException sqle ) {
			notice( "Db.getConnection database location ["
					+ linkTool
					+ "] db name["
					+ nameOfDatabase
					+ "]", sqle);
			finishOperate( );
		}
	}
	
	/**
	 * Opens database
	 * <p>
	 * Confirms database file exists and if so,
	 * loads JDBC driver and establishes JDBC connection to database
	 */
	private void checkDatabase() {
		File originalFile = new File(nameOfDatabase);

		if (!originalFile.exists()) {
			System.out.println(
				 "SQLite database file ["
				+ nameOfDatabase
				+ "] does not exist");
			System.exit( 0 );
		}
			try {
				Class.forName(JDBC_DRIVER);
				getConnection();
			} catch (ClassNotFoundException cnfe) {
				notice("Db.Open", cnfe);
			}

		   try {
			connection = DriverManager.getConnection(linkTool + originalDatabase);
			statement = connection.createStatement();
		   } catch (Exception e) {
			e.printStackTrace();
		   }

		if (mistake) {
			System.out.println( "Db.Open : leaving" );
		}
	}
	/**
	 * Close database
	 * <p>
	 * Commits any remaining updates to database and
	 * closes connection
	 */
	public final void finishOperate( ) {
		try {
			connection.commit( ); // Commit any updates
			connection.close ( );
		}
		catch ( Exception e ) {
			notice( "Db.close", e );
		}
	}

	/**
	 * Constructor
	 * <p>
	 * Records a copy of the database name and
	 * opens the database for use
	 *
	 */
	public DatabaseBasic() {
        imply();
		nameOfDatabase = originalDatabase;

		if (mistake) {
			System.out.println(
				  "Db.constructor ["
				+ nameOfDatabase
				+ "]");
		}

		checkDatabase();
	}

	/**提示操作*/
	private void imply(){
		Scanner scanner = new Scanner(System.in);
		System.out.print("Please enter the original database(eg: test1.db): ");
		//获取目标数据库
		this.originalDatabase = scanner.next();

		System.out.print("Please enter the type of file to save(eg:test1.sql or test1.txt): ");
		//创建目标数据库脚本文件
		this.statementsOfSql = scanner.next();

		System.out.print("Generate the backup of database(eg:test1backup.db): ");
		//创建副本数据库
		this.backupOperation = scanner.next();
		//关闭输入,节约内存
		scanner.close();

	}

}
