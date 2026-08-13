package scimma;

import javax.naming.AuthenticationNotSupportedException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramCredentialCallback;

import scimma.RestClient;
import scimma.PeriociallySyncable;
import scimma.SyncThread;

/**
 A class which provides Scram credentials for authentication from an external PostgreSQL database.
 */
public class ExternalScramAuthnCallbackHandler implements AuthenticateCallbackHandler,PeriociallySyncable {
	protected static final Logger LOG = LoggerFactory.getLogger(ExternalScramAuthnCallbackHandler.class);
	
	private String mechanism;
	
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
	private String externalAPIRoot = "http://localhost";
	private String externalAPIUsername = "KafkaAuth";
	private String externalAPIPassword = null; //no default!
	
	private RestClient client = null;
	
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
					else if(optionKey.equals("apiRoot") && option.getValue() instanceof String)
						externalAPIRoot=(String)option.getValue();
					else if(optionKey.equals("apiUsername") && option.getValue() instanceof String)
						externalAPIUsername=(String)option.getValue();
					else if(optionKey.equals("apiPassword") && option.getValue() instanceof String)
						externalAPIPassword=(String)option.getValue();
				}
			}
		}
		
		if(waitTime<1){
			String message="Invalid data store synchronization period "+Integer.toString(waitTime,10)+"; must be at least 1 second";
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}
		setSyncPeriod(waitTime);
		
		//client=new RestClient(externalAPIRoot, externalAPIUsername, externalAPIPassword);
		client=RestClient.clientForHost(externalAPIRoot, externalAPIUsername, externalAPIPassword);
		
		syncThread = new SyncThread(this);
		syncThread.start();
	}
	
	@Override
	public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
		String username = null;
		for (Callback callback : callbacks) {
			if (callback instanceof NameCallback)
				username = ((NameCallback) callback).getDefaultName();
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
			LOG.info("User "+username+" is on the blacklist");
			return null;
		}
		ScramCredential cred = credentials.get(username);
		if(cred==null){
			fetchCredentials(username);
			cred = credentials.get(username);
		}
		if(cred==null)
			LOG.info("User "+username+" not found");
		else
			LOG.info("Found credential for user "+username);
		return cred;
	}
	
	protected static void updateDataWithCredential(ConcurrentHashMap<String, ScramCredential> updatedCredentials, ConcurrentHashMap<String, Boolean> updatedBadUsernames, JSONObject cred){
		String username=cred.getString("username");
		if(cred.getBoolean("suspended")){
			//suspended credentials exist, but must be ignored
			updatedBadUsernames.put(username, true);
			updatedCredentials.remove(username);
		}
		else{
			Base64.Decoder b64d=Base64.getDecoder();
			byte[] salt = b64d.decode(cred.getString("salt"));
			byte[] serverKey = b64d.decode(cred.getString("server_key"));
			byte[] storedKey = b64d.decode(cred.getString("stored_key"));
			int iterations = cred.getInt("iterations");
			ScramCredential scred=new ScramCredential(salt,storedKey,serverKey,iterations);
			updatedBadUsernames.remove(username);
			updatedCredentials.put(username,scred);
		}
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
		LOG.debug("Looking up credential for user "+specificUser+" from hopauth API");
		try{
			RestClient.JSON cred=client.request("/v1/scram_credentials/"+specificUser);
			if(!cred.isObject()){
				LOG.warn("API response was not an object");
				return;
			}
			//insert directly into current data structures
			updateDataWithCredential(credentials, badUsernames, cred.getObject());
		}
		catch(IOException ex){
			LOG.warn("Failed to connect to hopauth API, lookup failed:\n"+ex.getMessage());
		}
	}
	
	protected void fetchCredentials(){
		LOG.debug("Looking up all credentials from hopauth API");
		try{
			ConcurrentHashMap<String, ScramCredential> updatedCredentials = new ConcurrentHashMap<String, ScramCredential>();
			ConcurrentHashMap<String, Boolean> updatedBadUsernames = new ConcurrentHashMap<String, Boolean>();
			RestClient.JSON creds=client.request("/v1/scram_credentials");
			if(!creds.isArray()){
				LOG.warn("API response was not a list");
				return;
			}
			LOG.info("Got "+Integer.toString(creds.getArray().length())+" credential records from hopauth API");
			for(int i=0; i<creds.getArray().length(); i++)
				updateDataWithCredential(updatedCredentials, updatedBadUsernames, creds.getArray().getJSONObject(i));
			//swap out the entire credential cache for the new one
			credentials = updatedCredentials;
			//replace the bad username list so it contains only known suspended credentials
			//this resets allowance for looking up as-yet-unknown users
			badUsernames = updatedBadUsernames;
		}
		catch(IOException ex){
			LOG.warn("Failed to connect to hopauth API, lookup failed:\n"+ex.getMessage());
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
		fetchCredentials();
	}
	
	public int getSyncPeriod(){ return syncPeriod; }
	public void setSyncPeriod(int period){
		if(period<0)
			throw new IllegalArgumentException("Invalid synchronization period: "+Integer.toString(period,10));
		syncPeriod=period;
	}
	
	@Override
	public void close(){
		if(syncThread!=null){
			syncThread.end();
			try{
				//Shut down sync thread
				//no longer in use
				LOG.debug("Sync thread joining");
				syncThread.join();
				LOG.debug("Sync thread joined");
			}
			catch(InterruptedException ex){
				LOG.debug("Sync thread interrupted");
			}
		}
		client.close();
	}
	
	public static void main(String[] args){
		//TODO: implement some tests?
		ExternalScramAuthnCallbackHandler test = new ExternalScramAuthnCallbackHandler();
		test.close();
	}
}
