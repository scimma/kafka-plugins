package scimma;

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
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramCredentialCallback;

import scimma.ExternalDataSource;
import scimma.ExternalDataSource.ConnectionWrapper;
import scimma.PeriociallySyncable;
import scimma.SyncThread;

/**
 A class which provides Scram credentials for authentication from an external PostgreSQL database.
 */
public class ExternalScramAuthnCallbackHandler implements AuthenticateCallbackHandler,PeriociallySyncable {
	protected static final Logger LOG = LoggerFactory.getLogger(ExternalScramAuthnCallbackHandler.class);
	
	private String mechanism;
	
	private ExternalDataSource dataSource = null;
	
	///A cache of valid credentials obtained during or 
	///since the last full synchronization with the database.
	///This cache is replaced with each re-synchronization. 
    private ConcurrentHashMap<String, ScramCredential> credentials;
	///A cache of credentials which either did not exist or were found to be suspended during or
	///since the last full synchronization with the database.
	///This cache is cleared with each re-synchronization. 
    private ConcurrentHashMap<String, Boolean> badUsernames;
	
    private SyncThread syncThread;
	private int syncPeriod;
	
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
		
		if(waitTime<1){
			String message="Invalid database synchronization period "+Integer.toString(waitTime,10)+"; must be at least 1 second";
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}
		setSyncPeriod(waitTime);
		
		dataSource = new ExternalDataSource(configs, configPrefix+"."+postgresConfigPrefix);
        
        syncThread = new SyncThread(this);
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
		LOG.debug("Looking up credential for user "+username);
        if(badUsernames.getOrDefault(username,false)){
            LOG.debug(username+" is on the blacklist");
            return null;
        }
        ScramCredential cred = credentials.get(username);
		if(cred==null){
            fetchCredentials(username);
			cred = credentials.get(username);
		}
        if(cred==null)
            LOG.debug("User "+username+" not found");
		return cred;
	}
    
	/**
	 Make an immediate check with the database for credentials.
	 This enables finding and authorizing credentials which are newly created since the last full 
	 database synchronization.
	 The result of the lookup is added to the appropriate cache. 
	 @param username if non-null, the name of a specific credential to look up.
	                 If null, all credentials will be loaded and the caches completely replaced. 
	 */
	protected void fetchCredentials(String specificUser){
		String query="SELECT username,salt,server_key,stored_key,iterations,suspended FROM hopskotch_auth_scramcredentials";
		if(specificUser!=null){
			query+=" WHERE username = ?";
			LOG.debug("Looking up credential for user "+specificUser+" in the database");
		}
		try(ConnectionWrapper conn=dataSource.getConnection(); 
			PreparedStatement st = conn.getConnection().prepareStatement(query);){
			if(specificUser!=null){
				//TODO: is any escaping or sanitization required, 
				//      since username is data originating from the client?
				st.setString(1, specificUser);
			}
            try(ResultSet rs = st.executeQuery()){
				ConcurrentHashMap<String, ScramCredential> updatedCredentials;
				ConcurrentHashMap<String, Boolean> updatedBadUsernames;
				if(specificUser==null){ //create new cache structures
					updatedCredentials = new ConcurrentHashMap<String, ScramCredential>();
					updatedBadUsernames = new ConcurrentHashMap<String, Boolean>();
				}
				else{ //update existing caches
					updatedCredentials = credentials;
					updatedBadUsernames = badUsernames;
				}
				
                if(rs.next()){
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
                else if(specificUser!=null){
                    //add username to blacklist to prevent making many repeated requests to the DB
                    badUsernames.put(specificUser, true);
                }
				
				if(specificUser==null){
					//swap out the entire credential cache for the new one
					credentials = updatedCredentials;
					//replace the bad username list so it contains only known suspended credentials
					//this resets allowance for looking up as-yet-unknown users
					badUsernames = updatedBadUsernames;
				}
            }
            catch(SQLException ex){
                LOG.warn("Failed to connect to database, lookup failed.\nSQL Error: "+ex.getMessage());
				dataSource.markConnectionBad();
            }
        }
        catch(SQLException ex){
            LOG.warn("Failed to connect to database, lookup failed.\nSQL Error: "+ex.getMessage());
			dataSource.markConnectionBad();
        }
    }
    
	/**
	 Replace all cached credential data with a full dataset read from the database. 
	 This overwrites the credentials and badUsernames cache, witht eh latter having 
	 all lookup failures removed but all known suspended credentials included.
	 This should be invoked only by the Handler's background SyncThread. 
	 */
	public void update(){
        LOG.debug("Synchronizing all credentials with the database");
		fetchCredentials(null);
    }
	
	public int getSyncPeriod(){ return syncPeriod; }
	public void setSyncPeriod(int period){
		if(period<0)
			throw new IllegalArgumentException("Invalid synchronization period: "+Integer.toString(period,10));
		syncPeriod=period;
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
		if(dataSource!=null)
			dataSource.close();
	}
	
	public static void main(String[] args){
		//TODO: implement some tests?
		ExternalScramAuthnCallbackHandler test = new ExternalScramAuthnCallbackHandler();
        test.close();
	}
}
