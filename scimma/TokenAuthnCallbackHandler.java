package scimma;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.JwtValidatorException; //requires Kafka 4.1?
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.secured.BasicOAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwks;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwksVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SerializedJwt;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONArray;
import org.json.JSONObject;

import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;

import scimma.RestClient;
import scimma.PeriociallySyncable;
import scimma.SyncThread;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

/**
 * SASL callback handler for bearer token authentication mapping user identifiers from an external
 * data source.
 */
public class TokenAuthnCallbackHandler implements AuthenticateCallbackHandler,PeriociallySyncable {
	private static final Logger log = LoggerFactory.getLogger(TokenAuthnCallbackHandler.class);
	
	private static final String configPrefix="TokenAuthnCallbackHandler.";
	private static final String TRUSTED_ISSUERS_CONFIG=configPrefix+"trusted.issuers";
	private static final String JWKS_CACHE_TTL_CONFIG=configPrefix+"jwks.cache.ttl.seconds";
	private static final String JWKS_REFRESH_INTERVAL_CONFIG=configPrefix+"jwks.refresh.interval.seconds";
	private static final String SUB_CLAIM_CONFIG=configPrefix+"sub.claim.name";
	private static final String SOURCE_PROPERTY_CONFIG=configPrefix+"source.property";
	private static final String TARGET_PROPERTY_CONFIG=configPrefix+"target.property";
	private static final String API_ROOT_CONFIG=configPrefix+"external.api.root";
	private static final String API_USERNAME_CONFIG=configPrefix+"external.api.useranme";
	private static final String API_PASSWORD_CONFIG=configPrefix+"external.api.password";
	private static final String SYNC_PERIOD_CONFIG=configPrefix+"sync.period.seconds";
	
	static final ConfigDef CONFIG_DEF = new ConfigDef()
		.define(TRUSTED_ISSUERS_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, "Comma separated list of trusted issuers")
		.define(JWKS_CACHE_TTL_CONFIG, ConfigDef.Type.INT, 3600, ConfigDef.Importance.MEDIUM, "Time JWKS results should be cached")
		.define(JWKS_REFRESH_INTERVAL_CONFIG, ConfigDef.Type.INT, 300, ConfigDef.Importance.LOW, "Minimum time interval to repeat attempting to fetch JWKS data from an issuer")
		.define(SUB_CLAIM_CONFIG, ConfigDef.Type.STRING, "sub", ConfigDef.Importance.MEDIUM, "Name of the token claim to use as the initial subject identifier")
		.define(SOURCE_PROPERTY_CONFIG, ConfigDef.Type.STRING, "email", ConfigDef.Importance.MEDIUM, "Name of property from which to map users")
		.define(TARGET_PROPERTY_CONFIG, ConfigDef.Type.STRING, "username", ConfigDef.Importance.MEDIUM, "Name of property to which to map users")
		.define(API_ROOT_CONFIG, ConfigDef.Type.STRING, "http://localhost", ConfigDef.Importance.HIGH, "Root of the external API to query")
		.define(API_USERNAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "External API username")
		.define(API_PASSWORD_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "External API password")
		.define(SYNC_PERIOD_CONFIG, ConfigDef.Type.INT, 300, ConfigDef.Importance.LOW, "Period is seconds between full synchronizations with the external API")
		;
	
	private HashSet<String> trustedIssuers;
	private long jwksCacheTtl;
	private long jwksRefreshInterval;
	
	private String scopeClaimName;
    private String subClaimName;
	
	private HashMap<String, RefreshingHttpsJwksVerificationKeyResolver> keyResolvers;
	//Used to process a JWT sufficiently to determine its clained issuer, so that full validation
	//can be dispatched to the correct consumer for full validation.
	JwtConsumer genericConsumer;
	private HashMap<String, JwtConsumer> jwtConsumers;
	
	private RestClient restClient;
	private SyncThread syncThread;
	private int syncPeriod;
	
	private String sourceProperty;
	private String targetProperty;
	
	private ConcurrentHashMap<String, String> userMapping;
	private ConcurrentHashMap<String, Boolean> badUsernames;

	public TokenAuthnCallbackHandler() {
	}

	@Override
	@SuppressWarnings("unchecked")
	public void configure(Map<String, ?> configs, String saslMechanism,
						 List<AppConfigurationEntry> jaasConfigEntries) {
		if(!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
			log.warn("Expected OAUTHBEARER mechanism, got: {}", saslMechanism);
		
		//Same as BorkerJwtValidator
		ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
		Integer clockSkew=cu.validateInteger(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, false);
		scopeClaimName=cu.validateString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        subClaimName=cu.validateString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);
		
		Map<String, Object> parsedConfigs = CONFIG_DEF.parse(configs);
		trustedIssuers=new HashSet<String>((List<String>)parsedConfigs.get(TRUSTED_ISSUERS_CONFIG));
		log.info("Number of trusted issuers: "+((Integer)trustedIssuers.size()).toString());
		jwksCacheTtl=(Integer)parsedConfigs.get(JWKS_CACHE_TTL_CONFIG);
		log.info("jwksCacheTtl: "+((Long)jwksCacheTtl).toString());
		jwksRefreshInterval=(Integer)parsedConfigs.get(JWKS_CACHE_TTL_CONFIG);
		
		//All JWTs must use a non-trivial JWS algorithm and an issued time, 
		//otherwise, validation details are per-issuer
		genericConsumer=new JwtConsumerBuilder()
			.setSkipAllValidators()
			.setSkipSignatureVerification()
			.build();
		
		keyResolvers=new HashMap<String, RefreshingHttpsJwksVerificationKeyResolver>();
		jwtConsumers=new HashMap<String, JwtConsumer>();
		for(String issuer : trustedIssuers){
			RefreshingHttpsJwksVerificationKeyResolver keyResolver =
			new RefreshingHttpsJwksVerificationKeyResolver(new RefreshingHttpsJwks(
				Time.SYSTEM, new HttpsJwks(issuer+"/.well-known/jwks.json"), jwksRefreshInterval*1000, jwksRefreshInterval*1000, jwksRefreshInterval*3*1000
			));
			keyResolver.configure(configs, saslMechanism, jaasConfigEntries);
			keyResolvers.put(issuer, keyResolver);
			
			JwtConsumerBuilder consumerBuilder=new JwtConsumerBuilder();
			consumerBuilder.setExpectedIssuer(issuer)
				.setJwsAlgorithmConstraints(DISALLOW_NONE)
				.setRequireIssuedAt()
				.setVerificationKeyResolver(keyResolver);
			if(clockSkew!=null)
				consumerBuilder.setAllowedClockSkewInSeconds(clockSkew);
			//TODO: support per-issuer sub and scope claim names
			
			jwtConsumers.put(issuer, consumerBuilder.build());
		}
		
		String externalAPIRoot=(String)parsedConfigs.get(API_ROOT_CONFIG);
		String externalAPIUsername=(String)parsedConfigs.get(API_USERNAME_CONFIG);
		String externalAPIPassword=(String)parsedConfigs.get(API_PASSWORD_CONFIG);
		restClient=RestClient.clientForHost(externalAPIRoot, externalAPIUsername, externalAPIPassword);
        
        userMapping=new ConcurrentHashMap<String, String>();
        badUsernames=new ConcurrentHashMap<String, Boolean>();
        
        sourceProperty=(String)parsedConfigs.get(SOURCE_PROPERTY_CONFIG);
        targetProperty=(String)parsedConfigs.get(TARGET_PROPERTY_CONFIG);
		
		int waitTime=(Integer)parsedConfigs.get(SYNC_PERIOD_CONFIG);
		if(waitTime<1){
			String message="Invalid data store synchronization period "+Integer.toString(waitTime,10)+"; must be at least 1 second";
			log.error(message);
			throw new IllegalArgumentException(message);
		}
		setSyncPeriod(waitTime);
		
		//this class currently cannot fetch data incrementally, so it needs an initial full-sync
		update();
		
		syncThread = new SyncThread(this);
        syncThread.start();
	}

	@Override
	public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
		for (Callback callback : callbacks) {
			if (callback instanceof OAuthBearerValidatorCallback) {
				handleValidatorCallback((OAuthBearerValidatorCallback) callback);
			} else {
				throw new UnsupportedCallbackException(callback,
					"Unsupported callback type: " + callback.getClass().getName());
			}
		}
	}

	private void handleValidatorCallback(OAuthBearerValidatorCallback callback) {
		String tokenValue = callback.tokenValue();

		if(tokenValue==null || tokenValue.trim().isEmpty()){
			log.warn("Received null or empty token value");
			callback.error("invalid_token", "Token value is null or empty", null);
			return;
		}
		
		try{
			OAuthBearerToken token=validate(tokenValue);
			if(token==null){
				callback.error("invalid_token", "Token validation returned null", null);
				return;
			}
			callback.token(token);
		}
		catch(JwtValidatorException ex){
			callback.error("invalid_token", ex.getMessage(), null);
		}
	}
	
	@SuppressWarnings("unchecked")
	public OAuthBearerToken validate(String accessToken) throws JwtValidatorException{
		SerializedJwt serializedJwt=new SerializedJwt(accessToken);
		//process with generic consumer for basic structure and to determine the claimed issuer
		JwtContext genericContext;
		try{
            genericContext=genericConsumer.process(serializedJwt.getToken());
        }
        catch(InvalidJwtException e){
            throw new JwtValidatorException("Invalid JWT: "+e.getMessage(), e);
        }
        String issuer;
        try{
        	issuer=genericContext.getJwtClaims().getIssuer();
        }
        catch(MalformedClaimException e){
            throw new JwtValidatorException("Invalid JWT: "+e.getMessage(), e);
        }
        
        //re-process based on issuer
        JwtConsumer consumer=jwtConsumers.get(issuer);
        if(consumer==null){
        	String message="Untrusted token issuer: "+issuer;
        	log.info(message);
        	throw new JwtValidatorException(message);
        }
        JwtContext jwt=null;
        try{
            jwt=consumer.process(serializedJwt.getToken());
        }
        catch(InvalidJwtException e){
        	e.printStackTrace(System.out);
            throw new JwtValidatorException("Invalid JWT: "+e.getMessage(), e);
        }
        catch(IllegalStateException e){
        	e.printStackTrace(System.out);
        	throw e;
        }
        
        JwtClaims claims=jwt.getJwtClaims();
        
        NumericDate issuedAtRaw = getClaim(claims::getIssuedAt, ReservedClaimNames.ISSUED_AT);
        Long issuedAt = (issuedAtRaw != null ? issuedAtRaw.getValueInMillis() : null);
        
        NumericDate expirationRaw = getClaim(claims::getExpirationTime, ReservedClaimNames.EXPIRATION_TIME);
        Long expiration = (expirationRaw != null ? expirationRaw.getValueInMillis() : null);
        
        String subRaw = getClaim(() -> claims.getStringClaimValue(subClaimName), subClaimName);
        
        Object scopeRaw = getClaim(() -> claims.getClaimValue(scopeClaimName), scopeClaimName);
        Collection<String> scopeRawCollection;
        if(scopeRaw instanceof String)
            scopeRawCollection = Collections.singletonList((String) scopeRaw);
        else if(scopeRaw instanceof Collection)
            scopeRawCollection = (Collection<String>) scopeRaw;
        else
            scopeRawCollection = Collections.emptySet();
        Set<String> scopes = ClaimValidationUtils.validateScopes(scopeClaimName, scopeRawCollection);
        
        if(badUsernames.getOrDefault(subRaw,false)){
        	log.info("Subject "+subRaw+" is on the blacklist");
        	throw new JwtValidatorException("Unacceptable token: Unknown subject");
        }
        String sub=userMapping.get(subRaw);
        if(sub==null){
        	log.info("Token user "+subRaw+" not found");
        	throw new JwtValidatorException("Unacceptable token: Unknown subject");
        }
        else
        	log.info("Mapped token for "+subRaw+" to user "+sub);
        //hacky: add a prefix to distinguish these identifiers from SCRAM credentials
        //SCRAM credentials are currently named from the local parts of email addresses,
        //so they cannot contain ':' characters, 
        sub = "User:"+sub;
        
        return new BasicOAuthBearerToken(accessToken,
                                         scopes,
                                         expiration,
                                         sub,
                                         issuedAt);
	}
	
	private <T> T getClaim(ClaimSupplier<T> supplier, String claimName) throws JwtValidatorException {
        try {
            T value = supplier.get();
            log.debug("getClaim - {}: {}", claimName, value);
            return value;
        } catch (MalformedClaimException e) {
            throw new JwtValidatorException(String.format("Could not extract the '%s' claim from the access token", claimName), e);
        }
    }
    
     public interface ClaimSupplier<T> {
        T get() throws MalformedClaimException;
    }
	
	@Override
	public void close(){
		jwtConsumers = null;
		for(Map.Entry<String, RefreshingHttpsJwksVerificationKeyResolver> entry : keyResolvers.entrySet())
			entry.getValue().close();
		keyResolvers = null;
	}
	
// 	//JwtValidatorException seems to first appear in Kafka 4.1, so include a copy here for compatibility with 4.0
// 	public class JwtValidatorException extends KafkaException {
// 		public JwtValidatorException(String message) {
// 			super(message);
// 		}
// 	
// 		public JwtValidatorException(Throwable cause) {
// 			super(cause);
// 		}
// 	
// 		public JwtValidatorException(String message, Throwable cause) {
// 			super(message, cause);
// 		}
// 	}
	
	protected void updateDataWithUserRecord(ConcurrentHashMap<String, String> updatedMapping, ConcurrentHashMap<String, Boolean> updatedBadUsernames, JSONObject record){
		String source=record.getString(sourceProperty);
		String target=record.getString(targetProperty);
		
		updatedBadUsernames.remove(source);
		updatedMapping.put(source,target);
    }
    
    protected void fetchUser(String specificUser){
		log.debug("Looking up data for user "+specificUser+" from hopauth API");
		try{
			RestClient.JSON data=restClient.request("/v1/users/"+specificUser);
			if(!data.isObject()){
				log.warn("API response was not an object");
				return;
			}
			//insert directly into current data structures
			updateDataWithUserRecord(userMapping, badUsernames, data.getObject());
		}
		catch(IOException ex){
			log.warn("Failed to connect to hopauth API, lookup failed:\n"+ex.getMessage());
		}
	}
	
	protected void fetchUsers(){
		log.debug("Looking up all users from hopauth API");
		try{
			ConcurrentHashMap<String, String> updatedMapping = new ConcurrentHashMap<String, String>();
			ConcurrentHashMap<String, Boolean> updatedBadUsernames = new ConcurrentHashMap<String, Boolean>();
			RestClient.JSON data=restClient.request("/v1/users");
			if(!data.isArray()){
				log.warn("API response was not a list");
				return;
			}
			log.info("Got "+Integer.toString(data.getArray().length())+" user records from hopauth API");
			for(int i=0; i<data.getArray().length(); i++)
				updateDataWithUserRecord(updatedMapping, updatedBadUsernames, data.getArray().getJSONObject(i));
			userMapping = updatedMapping;
			badUsernames = updatedBadUsernames;
		}
		catch(IOException ex){
			log.warn("Failed to connect to hopauth API, lookup failed:\n"+ex.getMessage());
		}
    }
	
	public void update(){
        log.debug("Synchronizing all users with the database");
		fetchUsers();
    }
    public int getSyncPeriod(){ return syncPeriod; }
	public void setSyncPeriod(int period){
		if(period<0)
			throw new IllegalArgumentException("Invalid synchronization period: "+Integer.toString(period,10));
		syncPeriod=period;
	}
}
