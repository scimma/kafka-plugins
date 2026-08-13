package scimma;

import org.apache.kafka.server.authorizer.Authorizer;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONArray;
import org.json.JSONObject;

import scimma.RestClient;
import scimma.PeriociallySyncable;
import scimma.SyncThread;

/**
 A class which makes authorization decisions based on rules stored externally in a PostgreSQL database. 
 */
public class ExternalAuthorizer implements Authorizer,PeriociallySyncable{
	protected static final Logger LOG = LoggerFactory.getLogger(ExternalAuthorizer.class);
	
	///The name of the configuration setting from which the names of super users will be obtained
	private static String superUsersProp = "super.users";
	///The prefix on all configuration entries specific to this class
	private static String configPrefix="ExternalAuthorizer";
	private String externalAPIRoot = "http://localhost";
	private String externalAPIUsername = "KafkaAuth";
	private String externalAPIPassword = null; //no default!
	
	///The set of all super users configured. 
	///Does not change after initial configuration, so concurrency-safe modification is not needed. 
	private HashSet<KafkaPrincipal> superUsers;
	///Map of topic names to whether they are know to definitely be or definitely not be publicly 
	///readable. 
	private ConcurrentHashMap<String, Boolean> publicTopics;
	///Map of user names to associated permissions. 
	///This permits fast determination of whether the permissions for a given user are known, 
	///and allows them to to be be concurrently updated (at user-level, not individual 
	///permission-level granularity). 
	private ConcurrentHashMap<String, HashSet<AclBinding>> ACLs;
	
	private RestClient client = null;
	
	private SyncThread syncThread;
	private int syncPeriod = 300; //seconds
	
	///Translation between scimma-admin's database representation of Kafka operations and the 
	///internal Kafka representation, since unfortunately they do not quite match. 
	private static HashMap<String, AclOperation> operationMap = new HashMap<String,AclOperation>();
	
	static {
		operationMap.put("All", AclOperation.ALL);
		operationMap.put("Read", AclOperation.READ);
		operationMap.put("Write", AclOperation.WRITE);
		operationMap.put("Create", AclOperation.CREATE);
		operationMap.put("Delete", AclOperation.DELETE);
		operationMap.put("Alter", AclOperation.ALTER);
		operationMap.put("Describe", AclOperation.DESCRIBE);
		operationMap.put("ClusterAction", AclOperation.CLUSTER_ACTION);
		operationMap.put("DescribeConfigs", AclOperation.DESCRIBE_CONFIGS);
		operationMap.put("AlterConfigs", AclOperation.ALTER_CONFIGS);
		operationMap.put("IdempotentWrite", AclOperation.IDEMPOTENT_WRITE);
	}
	
	public ExternalAuthorizer(){
		LOG.debug("ExternalAuthorizer constructed");
	}
	
	@Override
	public void configure(Map<String, ?> configs){
		
		superUsers = new HashSet<KafkaPrincipal>();
		Object rawSuperUsers=configs.get(superUsersProp);
		if(rawSuperUsers!=null && rawSuperUsers instanceof String){
			for(String user : ((String)rawSuperUsers).split(";")){
				LOG.info("Will recognize "+user+" as a superuser");
				superUsers.add(SecurityUtils.parseKafkaPrincipal(user.trim()));
			}
		}
		
		publicTopics = new ConcurrentHashMap<String, Boolean>();
		ACLs = new ConcurrentHashMap<String, HashSet<AclBinding>>();
		
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
			String message="Invalid database synchronization period "+Integer.toString(waitTime,10)+"; must be at least 1 second";
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}
		setSyncPeriod(waitTime);
		
		//client = new RestClient(externalAPIRoot, externalAPIUsername, externalAPIPassword);
		client = RestClient.clientForHost(externalAPIRoot, externalAPIUsername, externalAPIPassword);
		
		syncThread = new SyncThread(this);
		syncThread.start();
		
		LOG.info("ExternalAuthorizer configured");
	}
	
	public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo){
		Map<Endpoint, CompletableFuture<Void>> result = new HashMap<Endpoint, CompletableFuture<Void>>();
		for(Endpoint ep : serverInfo.endpoints()){
			CompletableFuture<Void> f=new CompletableFuture<Void>();
			f.complete(null);
			result.put(ep,f);
		}
		LOG.debug("ExternalAuthorizer started");
		return result;
	}
	
	public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions){
		LOG.debug("ExternalAuthorizer asked to authorize "+Integer.toString(actions.size())+" actions");
		ArrayList<AuthorizationResult> result=new ArrayList<AuthorizationResult>(actions.size());
		LOG.debug("  Action context is a request of type "+Integer.toString(requestContext.requestType()));
		
		if(superUsers.contains(requestContext.principal())){
			LOG.debug("  Request principal "+requestContext.principal().toString()+" is a superuser");
			for(Action action : actions){
				result.add(AuthorizationResult.ALLOWED); //super users can always do everything
				String messageBase="operation "+action.operation().toString()
					+" on "+action.resourcePattern().resourceType().toString()
					+" "+action.resourcePattern().name()
					+" by "+requestContext.principal().toString()
					+" from "+requestContext.clientAddress().toString();
				LOG.info("ALLOWED "+messageBase+" due to the principal being on the super-user list");
			}
			return result;
		}
		
		for(Action action : actions)
			result.add(authorize(requestContext, action));
		return result;
	}
	
	/**
	 Dispatch a single authorization request to the function appropriate for the subject type
	 */
	private AuthorizationResult authorize(AuthorizableRequestContext requestContext, Action action){
		switch(action.resourcePattern().resourceType()){
			case TOPIC:
				return authorizeTopicOperation(requestContext, action);
			case GROUP:
				return authorizeConsumerGroupOperation(requestContext, action);
			case CLUSTER:
				return authorizeClusterOperation(requestContext, action);
			case TRANSACTIONAL_ID:
				return authorizeTransactionOperation(requestContext, action);
			default: //No other object types are handled at this time
				LOG.warn("Rejected request for unsupported resource type: "+action.resourcePattern().resourceType().toString());
				return AuthorizationResult.DENIED;
		}
	}
	
	/**
	 Process an authorization request relating to a consumer group
	 @pre requestContext refers to a consumer group
	 */
	private AuthorizationResult authorizeConsumerGroupOperation(AuthorizableRequestContext requestContext, Action action){
		String messageBase="operation "+action.operation().toString()
			+" on consumer group "+action.resourcePattern().name()
			+" by "+requestContext.principal().toString()
			+" from "+requestContext.clientAddress().toString();
		LOG.debug("ExternalAuthorizer asked to authorize "+messageBase);
		//extract username
		String username=requestContext.principal().getName();
		//If the username has a "User:" prefix, denoting a user account rather than a SCRAM
		//credential, ignore it for the purpose of checking consumer group names.
		if(username.startsWith("User:") && username.length()>5)
			username=username.substring(5);
		String allowedPrefix=username+"-";
		//figure out subject name
		String subject=action.resourcePattern().name();
		if(subject.startsWith(allowedPrefix)){
			LOG.info("ALLOWED "+messageBase+" due to prefix match");
			return AuthorizationResult.ALLOWED;
		}
		LOG.info("DENIED "+messageBase);
		return AuthorizationResult.DENIED;
	}
	
	/**
	 Process an authorization request realting to a topic
	 @pre requestContext refers to a topic
	 */
	private AuthorizationResult authorizeTopicOperation(AuthorizableRequestContext requestContext, Action action){
		String messageBase="operation "+action.operation().toString()
			+" on topic "+action.resourcePattern().name()
			+" by "+requestContext.principal().toString()
			+" from "+requestContext.clientAddress().toString();
		LOG.debug("ExternalAuthorizer asked to authorize "+messageBase);
		String topic = action.resourcePattern().name();
		//treat the system __consumer_offsets topic specially
		if(topic.equals("__consumer_offsets")){
			LOG.info("ALLOWED "+messageBase+" by hard-coded rule");
			return AuthorizationResult.ALLOWED;
		}
		//if the action is a READ, and the subject is a public topic, we can authorize without needing to examine the principal
		if(isTopicPubliclyReadable(topic) && (action.operation()==AclOperation.READ || action.operation()==AclOperation.DESCRIBE)){
			LOG.info("ALLOWED "+messageBase+" due to the target being publicly readable");
			return AuthorizationResult.ALLOWED;
		}
		
		String username=requestContext.principal().getName();
		HashSet<AclBinding> userPerms=ACLs.get(username);
		if(userPerms==null){
			//try to fetch directly from the database
			gatherPermissions(username);
			userPerms=ACLs.get(username);
			if(userPerms==null){ //if still null there's a database issue, but we can't authorize
				LOG.info("DENIED "+messageBase+" due to lack of permission data");
				return AuthorizationResult.DENIED;
			}
		}
		
		//Since we do not use DENY permissions or anything but literal patterns, 
		//we can directly check if the required permission exists in one of two forms, 
		//namely the exact operation, or a blanket ALL permission
		ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
		AccessControlEntry entryExact = new AccessControlEntry(username,"*",action.operation(),AclPermissionType.ALLOW);
		if(userPerms.contains(new AclBinding(pattern, entryExact))){
			LOG.info("ALLOWED "+messageBase+" due to an exact permission match");
			return AuthorizationResult.ALLOWED;
		}
		AccessControlEntry entryGeneral = new AccessControlEntry(username,"*",AclOperation.ALL,AclPermissionType.ALLOW);
		if(userPerms.contains(new AclBinding(pattern, entryGeneral))){
			LOG.info("ALLOWED "+messageBase+" due to a match with an ALL permission rule");
			return AuthorizationResult.ALLOWED;
		}
		//As a special case, we interpret WRITE permission as also granting DESCRIBE_CONFIGS
		if(action.operation()==AclOperation.DESCRIBE_CONFIGS){
			AccessControlEntry entryWrite = new AccessControlEntry(username,"*",AclOperation.WRITE,AclPermissionType.ALLOW);
			if(userPerms.contains(new AclBinding(pattern, entryWrite))){
				LOG.info("ALLOWED "+messageBase+" due to a match with a corresponding WRITE permission rule");
				return AuthorizationResult.ALLOWED;
			}
		}
		
		//otherwise, we must deny
		LOG.info("DENIED "+messageBase+" due to lack of a matching permission");
		return AuthorizationResult.DENIED;
	}
	
	private AuthorizationResult authorizeClusterOperation(AuthorizableRequestContext requestContext, Action action){
		String messageBase="operation "+action.operation().toString()
			+" on cluster "+action.resourcePattern().name()
			+" by "+requestContext.principal().toString()
			+" from "+requestContext.clientAddress().toString();
		LOG.info("DENIED "+messageBase+" by default");
		return AuthorizationResult.DENIED;
	}
	
	private AuthorizationResult authorizeTransactionOperation(AuthorizableRequestContext requestContext, Action action){
		String messageBase="operation "+action.operation().toString()
			+" on transaction ID "+action.resourcePattern().name()
			+" by "+requestContext.principal().toString()
			+" from "+requestContext.clientAddress().toString();
		LOG.info("DENIED "+messageBase+" by default");
		return AuthorizationResult.DENIED;
	}
	
	/**
	 Look up whether a specific topic is publicly readable.
	 @return Whether the topic is publicly readable, and false if the status could not be 
	         definitely confirmed. 
	 */
	private boolean isTopicPubliclyReadable(String topic){
		Boolean isPublic = publicTopics.get(topic);
		if(isPublic==null){
			//try to fetch directly from the database
			checkTopic(topic);
			isPublic = publicTopics.get(topic);
			if(isPublic==null){ //if still null there's a database issue, so fail conservatively
				LOG.warn("Treating "+topic+" as not publicly readable due to lack of information");
				return false;
			}
		}
		return isPublic;
	}
	
	/**
	 Handle requests to create ACLs by rejecting them; all permissions must be modified via 
	 scimma-admin. 
	 */
	public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings){
		LOG.debug("ExternalAuthorizer asked to create "+Integer.toString(aclBindings.size())+" ACLs");
		ArrayList<CompletableFuture<AclCreateResult>> result=new ArrayList<CompletableFuture<AclCreateResult>>(aclBindings.size());
		for(int i=0; i<aclBindings.size(); i++){
			CompletableFuture<AclCreateResult> f=new CompletableFuture<AclCreateResult>();
			f.complete(new AclCreateResult(new ApiException("ACL creation is not supported")));
			result.add(f);
		}
		return result;
	}
	
	/**
	 Handle requests to delete ACLs by rejecting them; all permissions must be modified via 
	 scimma-admin. 
	 */
	public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters){
		LOG.debug("ExternalAuthorizer asked to delete "+Integer.toString(aclBindingFilters.size())+" ACLs");
		ArrayList<CompletableFuture<AclDeleteResult>> result=new ArrayList<CompletableFuture<AclDeleteResult>>(aclBindingFilters.size());
		for(int i=0; i<aclBindingFilters.size(); i++){
			CompletableFuture<AclDeleteResult> f=new CompletableFuture<AclDeleteResult>();
			f.complete(new AclDeleteResult(new ApiException("ACL deletion is not supported")));
			result.add(f);
		}
		return result;
	}
	
	/**
	 Report ACLs matching the given filter. 
	 */
	public Iterable<AclBinding> acls(AclBindingFilter filter){
		LOG.debug("ExternalAuthorizer asked to report ACLs");
		HashSet<AclBinding> results = new HashSet<AclBinding>();
		
		//scan all explicit ACLs for topics
		if(filter.patternFilter().resourceType()==ResourceType.TOPIC 
		   || filter.patternFilter().resourceType()==ResourceType.ANY){
			if(filter.entryFilter().principal()!=null){ //filter matches only a single user
				LOG.debug("  Will report explicit topic ACLs for user "+filter.entryFilter().principal());
				HashSet<AclBinding> userPerms=ACLs.get(filter.entryFilter().principal());
				if(userPerms!=null){
					for(AclBinding binding : userPerms){
						if(filter.matches(binding))
							results.add(binding);
					}
				}
			}
			else{ //filter matches any/all users
				LOG.debug("  Will report explicit topic ACLs for all users");
				for(Map.Entry<String, HashSet<AclBinding>> entry : ACLs.entrySet()){
					for(AclBinding binding : entry.getValue()){
						if(filter.matches(binding))
							results.add(binding);
					}
				}
			}
		}
		
		//scan public topics to find relevant implicit ACLs
		if(filter.patternFilter().resourceType()==ResourceType.TOPIC 
		   || filter.patternFilter().resourceType()==ResourceType.ANY){
			LOG.debug(" Will report implicit topic ACLs");
			Boolean publiclyReadable=publicTopics.get(filter.patternFilter().name());
			if(publiclyReadable!=null && publiclyReadable==true){
				ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, filter.patternFilter().name(), PatternType.LITERAL);
				AccessControlEntry entry = new AccessControlEntry("*","*",AclOperation.DESCRIBE,AclPermissionType.ALLOW);
				AclBinding implicitACL = new AclBinding(pattern, entry);
				if(filter.matches(implicitACL))
					results.add(implicitACL);
				entry = new AccessControlEntry("*","*",AclOperation.READ,AclPermissionType.ALLOW);
				implicitACL = new AclBinding(pattern, entry);
				if(filter.matches(implicitACL))
					results.add(implicitACL);
			}
		}
		
		//include relevant implicit ACLs for consumer groups
		if(filter.patternFilter().resourceType()==ResourceType.GROUP 
		   || filter.patternFilter().resourceType()==ResourceType.ANY){
			if(filter.entryFilter().principal()!=null){ //filter matches only a single user
				LOG.debug("  Will report implicit consumer group ACLs for user "+filter.entryFilter().principal());
				ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, filter.patternFilter().name(), PatternType.LITERAL);
				AccessControlEntry entry = new AccessControlEntry(filter.entryFilter().principal(),"*",AclOperation.ALL,AclPermissionType.ALLOW);
				AclBinding implicitACL = new AclBinding(pattern, entry);
				if(filter.matches(implicitACL))
					results.add(implicitACL);
			}
		}
		
		return results;
	}
	
	/**
	 Update all caches from the database. This will load all data, whether or not it has been 
	 previously used locally (permissions are fetched for all users). If communication with database 
	 fails, old cached data is retained.   
	 */
	public void update(){
		LOG.debug("Synchronizing all permissions with the database");
		gatherPermissions();
		checkTopics();
	}
	
	protected static void updateUserPermissionData(ConcurrentHashMap<String, HashSet<AclBinding>> updatedACLs, JSONObject perm, String usernameOverride){
		String username=(usernameOverride!=null ? usernameOverride : perm.getString("principal"));
		String topic=perm.getString("topic");
		String operation=perm.getString("operation");
		AclOperation kafkaOperation = operationMap.get(operation);
		LOG.debug("Found permission for user "+username+" to perform operation "+kafkaOperation.toString()+" on topic "+topic);
		ResourcePattern pattern=new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
		AccessControlEntry entry=new AccessControlEntry(username,"*",kafkaOperation,AclPermissionType.ALLOW);
		
		HashSet<AclBinding> userPerms=updatedACLs.get(username);
		if(userPerms==null){
			LOG.debug("   Creating new permission set for "+username);
			userPerms=new HashSet<AclBinding>();
			updatedACLs.put(username, userPerms);
		}
		userPerms.add(new AclBinding(pattern, entry));
		
		if(kafkaOperation!=AclOperation.ALL && kafkaOperation!=AclOperation.DESCRIBE){
			LOG.debug("Added implicit permission for user "+username+" to DESCRIBE on topic "+topic);
			entry=new AccessControlEntry(username,"*",AclOperation.DESCRIBE,AclPermissionType.ALLOW);
			userPerms.add(new AclBinding(pattern, entry));
		}
		LOG.debug("    Permission set for "+username+" now has "+Integer.toString(userPerms.size())+" entries");
	}
	
	private void gatherPermissions(String specificUser){
		LOG.debug("Looking up ACL data for user "+specificUser+" from hopauth API");
		try{
			RestClient.JSON perms;
			if(specificUser.startsWith("User:"))
				perms=client.request("/v1/users/"+specificUser.substring(5)+"/available_permissions");
			else //SCRAM credential identifiers do not have the "User:" prefix
				perms=client.request("/v1/credential_permissions/"+specificUser);
			if(!perms.isArray()){
				LOG.warn("API response was not a list");
				return;
			}
			for(int i=0; i<perms.getArray().length(); i++)
				updateUserPermissionData(ACLs, perms.getArray().getJSONObject(i), specificUser);
		}
		catch(IOException ex){
			LOG.warn("Failed to connect to hopauth API, ACL lookup failed:\n"+ex.getMessage());
		}
	}
	
	/**
	 Update all cached user permission data. This will load all data, whether or not it has been 
	 previously used locally (permissions are fetched for all users). If communication with database 
	 fails, old cached data is retained.   
	 */
	private void gatherPermissions(){
		LOG.debug("Looking up all ACL data from hopauth API");
		try{
			ConcurrentHashMap<String, HashSet<AclBinding>> updatedACLs=new ConcurrentHashMap<String, HashSet<AclBinding>>();
			RestClient.JSON perms=client.request("/v1/credential_permissions");
			if(!perms.isArray()){
				LOG.warn("API response was not a list");
				return;
			}
			LOG.debug("Got "+Integer.toString(perms.getArray().length())+" credential permission records from hopauth API");
			for(int i=0; i<perms.getArray().length(); i++)
				updateUserPermissionData(updatedACLs, perms.getArray().getJSONObject(i), null);
			
			perms=client.request("/v1/user_permissions");
			if(!perms.isArray()){
				LOG.warn("API response was not a list");
				return;
			}
			LOG.debug("Got "+Integer.toString(perms.getArray().length())+" user permission records from hopauth API");
			for(int i=0; i<perms.getArray().length(); i++){
				JSONObject perm=perms.getArray().getJSONObject(i);
				updateUserPermissionData(updatedACLs, perm, "User:"+perm.getString("principal"));
			}
			
			ACLs=updatedACLs;
		}
		catch(IOException ex){
			LOG.warn("Failed to connect to hopauth API, ACL lookup failed:\n"+ex.getMessage());
		}
	}
	
	/**
	 Update cached user permission data. If communication with database fails, old cached data is
	 retained.
	 @param specificUser A single user for which to look up permissions. 
	                     If null, all user permissions will be loaded. 
	 */
	
	private void checkTopic(String specificTopic){
		LOG.debug("Looking up topic metadata for "+specificTopic+" from hopauth API");
		try{
			RestClient.JSON data=client.request("/v1/topics/"+specificTopic);
			if(!data.isObject()){
				LOG.warn("API response was not an object");
				return;
			}
			boolean isPublic=data.getObject().getBoolean("publicly_readable");
			publicTopics.put(specificTopic, isPublic);
		}
		catch(IOException ex){
			LOG.warn("Failed to connect to hopauth API, topic lookup failed:\n"+ex.getMessage());
		}
	}
	
	private void checkTopics(){
		LOG.debug("Looking up metadata for all topics from hopauth API");
		try{
			ConcurrentHashMap<String, Boolean> updatedPublicTopics=new ConcurrentHashMap<String, Boolean>();
			RestClient.JSON data=client.request("/v1/topics");
			if(!data.isArray()){
				LOG.warn("API response was not an object");
				return;
			}
			LOG.debug("Got "+Integer.toString(data.getArray().length())+" topic records from hopauth API");
			for(int i=0; i<data.getArray().length(); i++){
				JSONObject topicData=data.getArray().getJSONObject(i);
				String topicName=topicData.getString("name");
				boolean isPublic=topicData.getBoolean("publicly_readable");
				updatedPublicTopics.put(topicName, isPublic);
			}
			publicTopics=updatedPublicTopics;
		}
		catch(IOException ex){
			LOG.warn("Failed to connect to hopauth API, topic lookup failed:\n"+ex.getMessage());
		}
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
		if(client!=null)
			client.close();
	}
}
