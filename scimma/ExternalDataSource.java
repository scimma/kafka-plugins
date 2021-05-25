package scimma;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class ExternalDataSource implements Closeable{
	protected static final Logger LOG = LoggerFactory.getLogger(ExternalDataSource.class);
	
	///The PostgreSQL server
	private String databaseHost = "localhost";
	///The name of the PostgreSQL database to which to connect
	private String databaseName = "scimma";
	///The name of the PostgreSQL username to use
	private String databaseUser = "scimma_user";
	///The name of the PostgreSQL password to use
	private String databasePassword = null; //no default!
	///Whether to use SSL transport security when communicating with the database
	private boolean databaseSSL = true;
	
	///A lock protecting the database connection object
	private Lock connectionLock;
	///The connection to PostgreSQL. 
	///May be null, to indicate that the last attempt to communicate with the database failed, 
	///and a new connection must be made.
	///connectionLock must be held to manipulate this object. 
	private Connection databaseConnection = null;
	
	ExternalDataSource(String dbHost, String dbName, String dbUser, String dbPassword, boolean dbSSL){
		databaseHost=dbHost;
		databaseName=dbName;
		databaseUser=dbUser;
		databasePassword=dbPassword;
		databaseSSL=dbSSL;
		
		this.connectionLock = new ReentrantLock();
		
		try{
			connect();
		}
		catch(SQLException ex){
			//Demand that we can connect with the database initially.
			//We can tolerate temporary communication outages later, but we want to verify that 
			//things can work before letting anynoe believe that the server is n a good long-
			//term state. 
			String message="Failed to connect to PostgreSQL database: "+ex.getMessage();
			LOG.error(message);
			throw new RuntimeException(message);
		}
	}
	
	ExternalDataSource(Map<String, ?> configs, String configPrefix){
		String databaseSSL = "true";
		
		if(configs != null){
            for(Map.Entry<String,?> option: configs.entrySet()){
				if(option.getKey().startsWith(configPrefix)){
					
					if(option.getKey().length()<=configPrefix.length()+1 || 
					   option.getKey().charAt(configPrefix.length())!='.')
						continue;
					String optionKey=option.getKey().substring(configPrefix.length()+1);
						
					if(optionKey.equals("host") && option.getValue() instanceof String)
						databaseHost=(String)option.getValue();
					if(optionKey.equals("database") && option.getValue() instanceof String)
						databaseName=(String)option.getValue();
					if(optionKey.equals("user") && option.getValue() instanceof String)
						databaseUser=(String)option.getValue();
					if(optionKey.equals("password") && option.getValue() instanceof String)
						databasePassword=(String)option.getValue();
					if(optionKey.equals("ssl") && option.getValue() instanceof String)
						databaseSSL=(String)option.getValue();
				}
            }
        }
		
		if(databasePassword==null){
			String message="Database password ("+configPrefix+".password) not configured";
			LOG.error(message);
			throw new RuntimeException(message);
		}
        
		if(!databaseSSL.equals("true") && !databaseSSL.equals("false")){
			String message="Invalid database SSL setting ("+configPrefix+".ssl): "+databaseSSL+"; expected 'true' or 'false'";
			LOG.error(message);
            throw new IllegalArgumentException(message);
		}
		this.databaseSSL=Boolean.parseBoolean(databaseSSL);
		
		this.connectionLock = new ReentrantLock();
		
		try{
			connect();
		}
		catch(SQLException ex){
			//Demand that we can connect with the database initially.
			//We can tolerate temporary communication outages later, but we want to verify that 
			//things can work before letting anynoe believe that the server is n a good long-
			//term state. 
			String message="Failed to connect to PostgreSQL database: "+ex.getMessage();
			LOG.error(message);
			throw new RuntimeException(message);
		}
	}
	
	/**
	 Attempt to form a connection to the PostgreSQL database. 
	 This will safely do nothing if the connection already exists, 
	 although it will not verify that the connection is still working.
	 @throws SQLException from the underlying database backend if an error occurs. 
	 */
	private void connect() throws SQLException{
		if(databaseConnection!=null)
			return;
		
		String databaseURL = "jdbc:postgresql://"+databaseHost+"/"+databaseName;
		Properties databaseProps = new Properties();
		databaseProps.setProperty("user",databaseUser);
		databaseProps.setProperty("password",databasePassword);
		if(!databaseSSL)
			databaseProps.setProperty("ssl",Boolean.toString(databaseSSL));
		//TODO: without setting sslmode and possibly the server cert, the validity of the server 
		//      cert and matching of the hostname will not be checked!
		//      sslmode = verify-full is probably the right long-term choice
		///     See https://jdbc.postgresql.org/documentation/head/ssl-client.html
		try(LockGuard g=new LockGuard(connectionLock)){
			LOG.info("(Re)connecting to PostgreSQL database at "+databaseURL.toString());
			databaseConnection = DriverManager.getConnection(databaseURL, databaseProps);
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to PostgreSQL database: "+ex.getMessage());
			markConnectionBad();
			throw ex;
		}
	}
	
	/**
	 Closes down the database connection if it exists.
	 */
	private void disconnect(){
		if(databaseConnection==null)
			return;
		try(LockGuard g=new LockGuard(connectionLock)){
			databaseConnection.close();
			markConnectionBad();
		}
		catch(SQLException ex){
			LOG.warn("SQL Error: "+ex.getMessage());
		}
	}
	
	/**
	 Ensures that a suitable lock is held while the database connection is in use. 
	 */
	public class ConnectionWrapper extends LockGuard{
		private Connection databaseConnection = null;
		public ConnectionWrapper(Lock l, Connection conn){
			super(l);
			databaseConnection=conn;
		}
		/**
		 Get the connection object. Callers must not hold references to the connection without also 
		 holding a reference to this guard object. 
		 */
		public Connection getConnection(){ return databaseConnection; }
	}
	
	/**
	 Get the database connection while holding an appropriate lock. 
	 */
	public ConnectionWrapper getConnection() throws SQLException{
		connect(); //first attempt to ensure that the database connection is valid
		return new ConnectionWrapper(connectionLock,databaseConnection);
	}
	
	/**
	 Un-set the database connection because it has failed in some way, so that we will try to 
	 re-open it on the next use. 
	 */
	public void markConnectionBad(){ databaseConnection=null; }
	
	@Override
	public void close(){
		disconnect(); //close DB connection for good
	}
}
