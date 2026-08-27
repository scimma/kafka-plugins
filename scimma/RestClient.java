package scimma;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.security.scram.internals.ScramSaslClient;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

public class RestClient implements Closeable{
	protected static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

	private String externalAPIRoot = "http://localhost";
	private String externalAPIUsername = "KafkaAuth";
	private String externalAPIPassword = null; //no default!
	private CallbackHandler credential = null;
	
	///Must be held to update token/tokenExpiration
	private Lock tokenLock;
	private String token=null;
	private Instant tokenExpiration;
	
	private static ConcurrentHashMap<String, RestClient> clients = new ConcurrentHashMap<String, RestClient>();
	
	private class CallbackHandler implements javax.security.auth.callback.CallbackHandler{
		private String username;
		private String password;
		
		public CallbackHandler(String username, String password){
			this.username=username;
			this.password=password;
		}
		
		public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException{
			for(Callback callback : callbacks){
				if(callback instanceof NameCallback)
					((NameCallback)callback).setName(username);
				if(callback instanceof PasswordCallback)
					((PasswordCallback)callback).setPassword(password.toCharArray());
			}
		}
	};
	
	RestClient(String apiRoot, String apiUsername, String apiPassword){
		externalAPIRoot=apiRoot;
		externalAPIUsername=apiUsername;
		externalAPIPassword=apiPassword;
		credential=new CallbackHandler(apiUsername, apiPassword);
		tokenLock=new ReentrantLock();
		tokenExpiration=Instant.now();
	}
	
	/**
	 * Factory function which returns a shared RestClient instance to be used with the specified
	 * API host.
	 */
	public static RestClient clientForHost(String apiRoot, String apiUsername, String apiPassword){
		return clients.computeIfAbsent(apiRoot, (k)->new RestClient(apiRoot, apiUsername, apiPassword));
	}
	
	private static String stringFromStream(InputStream istream) throws IOException{
		int bufferSize = 1024;
		char[] buffer = new char[bufferSize];
		StringBuilder result=new StringBuilder();
		InputStreamReader reader=new InputStreamReader(istream, StandardCharsets.UTF_8);
		int readSize;
		while((readSize=reader.read(buffer, 0, buffer.length))>0)
			result.append(buffer, 0, readSize);
		reader.close();
		istream.close();
		return result.toString();
	}
	
	public class JSON{
		public JSON(JSONArray a){
			array=a;
			object=null;
		}
		public JSON(JSONObject o){
			array=null;
			object=o;
		}
		public boolean isArray(){ return array!=null; }
		public boolean isObject(){ return object!=null; }
		public JSONArray getArray(){ return array; }
		public JSONObject getObject(){ return object; }
		public String toString(){
			if(array!=null)
				return array.toString();
			return object.toString();
		}
		
		private JSONArray array;
		private JSONObject object;
	}
	
	private String getToken() throws IOException{
		try(LockGuard g=new LockGuard(tokenLock)){
			//if we don't have a token, or the token will expire in less than 30 seconds,
			//get a new one
			if(token==null || tokenExpiration.isBefore(Instant.now().plusSeconds(30))){
				ScramSaslClient scramClient=new ScramSaslClient(ScramMechanism.SCRAM_SHA_512, credential);
				byte[] empty={};
				byte[] clientFirst=scramClient.evaluateChallenge(empty);
				
				HttpURLConnection conn=connectionForURL(externalAPIRoot+"/v1/scram/first");
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/json");
				conn.setDoOutput(true);
				
				JSONObject firstRequestBody=new JSONObject().put("client_first", new String(clientFirst));
				conn.connect();
				OutputStream connOut=conn.getOutputStream();
				connOut.write(firstRequestBody.toString().getBytes("UTF-8"));
				connOut.close();
				
				String firstResult=stringFromStream(conn.getInputStream());
				if(conn.getResponseCode()>299)
					throw new IOException("SCRAM first request failed with status "+
					                      Integer.toString(conn.getResponseCode())+
					                      ": "+firstResult);
				JSONObject firstJSON = new JSONObject(firstResult);
				if(!firstJSON.has("server_first") || !firstJSON.has("sid"))
					throw new IOException("Malformed SCRAM first response: "+firstResult);
				
				byte[] clientFinal=scramClient.evaluateChallenge(firstJSON.getString("server_first").getBytes());
				conn=connectionForURL(externalAPIRoot+"/v1/scram/final");
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/json");
				conn.setDoOutput(true);
				
				JSONObject finalRequestBody=new JSONObject().put("client_final", new String(clientFinal))
				                                            .put("sid", firstJSON.getString("sid"));
				conn.connect();
				connOut=conn.getOutputStream();
				connOut.write(finalRequestBody.toString().getBytes("UTF-8"));
				connOut.close();
				String finalResult=stringFromStream(conn.getInputStream());
				if(conn.getResponseCode()>299)
					throw new IOException("SCRAM final request failed with status "+
					                      Integer.toString(conn.getResponseCode())+
					                      ": "+finalResult);
				JSONObject finalJSON = new JSONObject(finalResult);
				if(!finalJSON.has("server_final") || !finalJSON.has("sid") || !finalJSON.has("token") || !finalJSON.has("token_expires"))
					throw new IOException("Malformed SCRAM final response: "+finalResult);
				//validate completion of SCRAM handshake before trusting the resulting token
				scramClient.evaluateChallenge(finalJSON.getString("server_final").getBytes());
				
				token="Token "+finalJSON.getString("token");
				tokenExpiration=Instant.parse(finalJSON.getString("token_expires"));
				LOG.debug("Got new token: "+token);

			}
			return token;
		}
		catch(NoSuchAlgorithmException ex){
			throw new IOException("Unsupported SCRAM mechanism: "+ex.getMessage());
		}
		catch(SaslException ex){
			throw new IOException("SCRAM handshake failed, unable to get rEST token: "+ex.getMessage());
		}
	}
	
	private HttpURLConnection connectionForURL(String rawURL) throws IOException{
		URL url=null;
		try{
			url=new URI(rawURL).toURL();
		}
		catch(URISyntaxException ex){
			throw new IOException("Malformed URL: "+rawURL);
		}
		HttpURLConnection conn=(HttpURLConnection)url.openConnection();
		conn.setFollowRedirects(true);
		conn.setConnectTimeout(1000);
		conn.setReadTimeout(5000);
		return conn;
	}
	
	public JSON request(String path) throws IOException{
		String curToken=getToken();
		LOG.debug("Making GET request to "+externalAPIRoot+path+" with Authorization: "+curToken);
		
		HttpURLConnection conn=connectionForURL(externalAPIRoot+path);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Authorization", curToken);
		conn.connect();
		
		if(conn.getResponseCode()>=200 && conn.getResponseCode()<=299){
			var tok=new JSONTokener(conn.getInputStream());
			char first=tok.next();
			tok.back(); //un-consume first character so it is available to other parsers
			if(first=='{')
				return new JSON(new JSONObject(tok));
			else if(first=='[')
				return new JSON(new JSONArray(tok));
			else{
				int bufferSize = 1024;
				char[] buffer = new char[bufferSize];
				StringBuilder raw=new StringBuilder();
				while(tok.more()) //this is very inefficient, but should be used rarely
					raw.append(tok.next());
				throw new IOException("Response body does not appear to be JSON: "+raw.toString());
			}
		}
		else
			throw new IOException("Request failed with status "+
			                      Integer.toString(conn.getResponseCode())+
			                      ": "+conn.getResponseMessage());
	}
	
	@Override
	public void close(){
		// nothing to do
	}
	
	public static void main(String[] args){
		if(args.length<4){
			System.out.println("Required arguments: username password api_root request_path");
			return;
		}
		RestClient client=new RestClient(args[2], args[0], args[1]);
		try{
			System.out.println(client.request(args[3]).toString());
		}
		catch(IOException ex){
			System.out.println("Request failed: "+ex.getMessage());
		}
	}
}