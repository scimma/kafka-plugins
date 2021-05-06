import javax.naming.AuthenticationNotSupportedException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramCredentialCallback;

/**
 A class which provides Scram credentials for authentication from an external PostgreSQL database.
 */
public class ExternalScramAuthnCallbackHandler implements AuthenticateCallbackHandler {
	protected static final Logger LOG = LoggerFactory.getLogger(ExternalScramAuthnCallbackHandler.class);
	
	private String mechanism;
	
	///The PostgreSQL server
	private String databaseHost = "localhost";
	///The name of the PostgreSQL database to which to connect
	private String databaseName = "scimma";
	///The name of the PostgreSQL username to use
	private String databaseUser = "scimma_user";
	///The name of the PostgreSQL password to use
	private String databasePassword = null; //no default!
	///Whether to use SSL transport security when communicating with the database
	private String databaseSSL = "true";
	
	///A lock protecting the database connection object
	private Lock connectionLock;
	///The connection to PostgreSQL. 
	///May be null, to indicate that the last attempt to communicate with the database failed, 
	///and a new connection must be made.
	///connectionLock must be held to manipulate this object. 
    private Connection databaseConnection = null;
	///A cache of valid credentials obtained during or 
	///since the last full synchronization with the database.
	///This cache is replaced with each re-synchronization. 
    private ConcurrentHashMap<String, ScramCredential> credentials;
	///A cache of credentials which either did not exist or were found to be suspended during or
	///since the last full synchronization with the database.
	///This cache is cleared with each re-synchronization. 
    private ConcurrentHashMap<String, Boolean> badUsernames;
    
	///An equivalent to std::lock_guard to allow using try-with-resources blocks to manage Locks.
	private static class LockGuard implements AutoCloseable{
		private Lock lock;
		public LockGuard(Lock l){
			this.lock=l;
			this.lock.lock();
		}
		public void close(){
			this.lock.unlock();
		}
	}
	
	///A background thread object to drive periodic full synchronizations with the database.
    private static class SyncThread extends Thread{
		///The handler object for which this thread triggers re-synchronization.
        private ExternalScramAuthnCallbackHandler handler;
		///A lock for managing orderly shutdown of the thread
        private Lock lock;
		///A condition variable based on lock, on which the thread will wait while sleeping.
		///lock must be held to manipulte this object.
        private Condition stopCondition;
		///A flag which indicates that the thread should cease running.
		///lock must be held to manipulate this object after construction. 
        private boolean stopFlag;
		///The length of time to wait between updates, in seconds.
		private int waitTime;
		
        SyncThread(ExternalScramAuthnCallbackHandler handler, int waitTime){
            this.handler = handler;
            this.lock = new ReentrantLock();
            this.stopCondition = lock.newCondition();
            this.stopFlag = false;
			this.waitTime = waitTime;
        }
        public void run(){
            LOG.debug("Sync thread started");
            try{
                //Kafka may end up starting a number of these threads, very close to the same time,
                //so include a random delay to try to keep database requests from being in phase.
				try(LockGuard g=new LockGuard(lock)){
					LOG.debug("Sync thread entering initial random sleep");
					if(stopCondition.await((int)(Math.random()*30), TimeUnit.SECONDS))
                        return;
                }
                //main update loop
                while(true){
                    handler.updateAllCredentials();
					//check for instructions to terminate, and sleep before the next update.
					try(LockGuard g=new LockGuard(lock)){
						if(stopFlag)
                            return;
						if(stopCondition.await(waitTime, TimeUnit.SECONDS))
                            return;
                    }
                }
            }
            catch(InterruptedException ex){
                LOG.debug("Sync thread interrupted");
            }
        }
        //can't name this stop because the base version is final, 
        //even though it's unsafe, deprecated, and should never be used
        public void end(){
            LOG.debug("Sync thread stopping");
			try(LockGuard g=new LockGuard(lock)){
                stopFlag = true;
                stopCondition.signal();
            }
        }
    }
	
    private SyncThread syncThread;
	
	private static String configPrefix="ExternalScramAuthnCallbackHandler";
	private static String postgresConfigPrefix="postgres";
	
	@Override
	public void configure(Map<String, ?> configs, String saslMechanism, 
                          List<AppConfigurationEntry> jaasConfigEntries){
        this.mechanism = saslMechanism;
        
        credentials = new ConcurrentHashMap<String, ScramCredential>();
        badUsernames = new ConcurrentHashMap<String, Boolean>();
		
		int waitTime = 300; //seconds = 5 minutes
        
        if(configs != null){
            for(Map.Entry<String,?> option: configs.entrySet()){
				if(option.getKey().startsWith(configPrefix)){
					
					if(option.getKey().length()<=configPrefix.length()+1 || 
					   option.getKey().charAt(configPrefix.length())!='.')
						continue;
					String optionKey=option.getKey().substring(configPrefix.length()+1);
					
					if(optionKey.startsWith(postgresConfigPrefix)){
						// handle database options
						if(optionKey.length()<=postgresConfigPrefix.length()+1 || 
						   optionKey.charAt(postgresConfigPrefix.length())!='.')
							continue;
						String postgresKey=optionKey.substring(postgresConfigPrefix.length()+1);
						
						if(postgresKey.equals("host") && option.getValue() instanceof String)
							databaseHost=(String)option.getValue();
						if(postgresKey.equals("database") && option.getValue() instanceof String)
							databaseName=(String)option.getValue();
						if(postgresKey.equals("user") && option.getValue() instanceof String)
							databaseUser=(String)option.getValue();
						if(postgresKey.equals("password") && option.getValue() instanceof String)
							databasePassword=(String)option.getValue();
						if(postgresKey.equals("ssl") && option.getValue() instanceof String)
							databaseSSL=(String)option.getValue();
					}
					else{
						if(optionKey.equals("syncPeriod") && option.getValue() instanceof String){
							try{
								waitTime=Integer.parseInt((String)option.getValue(),10);
							}
							catch(NumberFormatException ex){
								String message="Invalid database synchronization period "+
									(String)option.getValue()+
									" could not be interpreted as an integer";
								LOG.error(message);
								throw new IllegalArgumentException(message);
							}
						}
					}
				}
            }
        }
		
		if(databasePassword==null){
			String message="Database password ("+configPrefix+"."+postgresConfigPrefix+".password) not configured";
			LOG.error(message);
			throw new RuntimeException(message);
		}
        
		if(!databaseSSL.equals("true") && !databaseSSL.equals("false")){
			String message="Invalid database SSL setting "+databaseSSL+"; expected 'true' or 'false'";
			LOG.error(message);
            throw new IllegalArgumentException(message);
		}
		
		if(waitTime<1){
			String message="Invalid database synchronization period "+Integer.toString(waitTime,10)+"; must be at least 1 second";
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}
        
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
        
        syncThread = new SyncThread(this, waitTime);
        syncThread.start();
	}
	
	@Override
	public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
		String username = null;
		for (Callback callback : callbacks) {
			if (callback instanceof NameCallback){
                username = ((NameCallback) callback).getDefaultName();
			}
			else if (callback instanceof ScramCredentialCallback)
				((ScramCredentialCallback) callback).scramCredential(credential(username));
			else
				throw new UnsupportedCallbackException(callback);
		}
	}
    
	/**
	 Look up the credential, if any, associated with a username. 
	 Credentials will be returned from the cache when possible, to reduce traffic to the database.
	 @param username the name of the credential to look up.
	 @return the corresponding credential, or null if no valid credential is known. 
	 */
	protected ScramCredential credential(String username) {
		// Return SCRAM credential from credential store
		LOG.debug("ExternalScramAuthnCallbackHandler: Looking up credential for user "+username);
        if(badUsernames.getOrDefault(username,false)){
            LOG.debug(username+" is on the blacklist");
            return null;
        }
        ScramCredential cred = credentials.get(username);
        if(cred==null)
            cred = fetchSingleCredentialSynchronous(username);
        if(cred==null)
            LOG.debug("User not found");
		return cred;
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
		if(!databaseSSL.equals("false"))
			databaseProps.setProperty("ssl",databaseSSL);
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
			databaseConnection=null;
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
			databaseConnection=null;
		}
		catch(SQLException ex){
			LOG.warn("SQL Error: "+ex.getMessage());
		}
	}
    
	/**
	 Make an immediate check with the database for a specific credential.
	 This enables finding and authorizing credentials which are newly created since the last full 
	 database synchronization.
	 The result of the lookup is added to the appropriate cache. 
	 @param username the name of the credential to look up
	 @return the credential which was found, or null if no valid credential exists
	 */
    protected ScramCredential fetchSingleCredentialSynchronous(String username){
        LOG.debug("ExternalScramAuthnCallbackHandler: Looking up credential for user "+username+" in the database");
		try{ //first attempt to ensure that the database connection is valid
			connect();
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to database, lookup failed.\nSQL Error: "+ex.getMessage());
			return null;
		}
		try(LockGuard g=new LockGuard(connectionLock); 
			PreparedStatement st = databaseConnection.prepareStatement(
				"SELECT salt,server_key,stored_key,iterations,suspended FROM hopskotch_auth_scramcredentials WHERE username = ?");
			){
            //TODO: is any escaping or sanitization required, 
            //      since username is data originating from the client?
            st.setString(1, username);
            try(ResultSet rs = st.executeQuery()){
                if(rs.next()){ //ugly, but required
                    byte[] salt = rs.getBytes("salt");
                    byte[] serverKey = rs.getBytes("server_key");
                    byte[] storedKey = rs.getBytes("stored_key");
                    int iterations = rs.getInt("iterations");
                    boolean suspended = rs.getBoolean("suspended");
                    
                    if(suspended){ //suspended credentials exist, but must be ignored
                        badUsernames.put(username, true);
                        return null;
                    }
                    
                    ScramCredential cred = new ScramCredential(salt,storedKey,serverKey,iterations);
                    credentials.put(username, cred); //add to cache
                    return cred;
                }
                else{
                    //add username to blacklist to prevent making many repeated requests to the DB
                    badUsernames.put(username, true);
                }
            }
            catch(SQLException ex){
                LOG.warn("Failed to connect to database, lookup failed.\nSQL Error: "+ex.getMessage());
				databaseConnection=null; //mark connection bad
                return null;
            }
        }
        catch(SQLException ex){
            LOG.warn("Failed to connect to database, lookup failed.\nSQL Error: "+ex.getMessage());
			databaseConnection=null; //mark connection bad
            return null;
        }
        LOG.debug("User "+username+" not found in database");
        return null;
    }
    
	/**
	 Replace all cached credential data with a full dataset read from the database. 
	 This overwrites the credentials and badUsernames cache, witht eh latter having 
	 all lookup failures removed but all known suspended credentials included.
	 This should be invoked only by the Handler's background SyncThread. 
	 */
    protected void updateAllCredentials(){
        LOG.debug("ExternalScramAuthnCallbackHandler: synchronizing all credentials with the database");
		try{ //first attempt to ensure that the database connection is valid
			connect();
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
			return;
		}
		try(LockGuard g=new LockGuard(connectionLock);
			PreparedStatement st = databaseConnection.prepareStatement(
				"SELECT username,salt,server_key,stored_key,iterations,suspended FROM hopskotch_auth_scramcredentials");
			){
            try(ResultSet rs = st.executeQuery()){
                ConcurrentHashMap<String, ScramCredential> updatedCredentials = new ConcurrentHashMap<String, ScramCredential>();
                ConcurrentHashMap<String, Boolean> updatedBadUsernames = new ConcurrentHashMap<String, Boolean>();
                
                while(rs.next()){ //ugly, but required
                    String username = rs.getString("username");
                    byte[] salt = rs.getBytes("salt");
                    byte[] serverKey = rs.getBytes("server_key");
                    byte[] storedKey = rs.getBytes("stored_key");
                    int iterations = rs.getInt("iterations");
                    boolean suspended = rs.getBoolean("suspended");
                    
                    if(suspended) //suspended credentials exist, but must be ignored
                        updatedBadUsernames.put(username, true);
                    else
                        updatedCredentials.put(username, new ScramCredential(salt,storedKey,serverKey,iterations));
                }
                
                //swap out the entire credential cache for the new one
                credentials = updatedCredentials;
                //replace the bad username list so it contains only known suspended credentials
                //this resets allowance for looking up as-yet-unknown users
                badUsernames = updatedBadUsernames;
            }
            catch(SQLException ex){
                LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
				databaseConnection=null; //mark connection bad
                return;
            }
        }
        catch(SQLException ex){
            LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
			databaseConnection=null; //mark connection bad
			return;
        }
    }
    
	@Override
	public void close() {
		if(syncThread!=null){
			syncThread.end();
			try{
				//must join the sync thread before closing the database connection to ensure it is 
				//no longer in use
				LOG.debug("Sync thread joining");
				syncThread.join();
				LOG.debug("Sync thread joined");
			}
			catch(InterruptedException ex){
				LOG.debug("Sync thread interrupted");
			}
		}
		disconnect(); //close DB connection for good
	}
	
	public static void main(String[] args){
		//TODO: implement some tests?
		ExternalScramAuthnCallbackHandler test = new ExternalScramAuthnCallbackHandler();
        test.close();
	}
}
