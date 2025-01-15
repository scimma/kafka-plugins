package scimma;

import org.apache.kafka.server.authorizer.Authorizer;

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

import java.sql.*;

import scimma.ExternalDataSource;
import scimma.ExternalDataSource.ConnectionWrapper;
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
	///The subsidiary prefix for database configuration options for this class
	private static String postgresConfigPrefix="postgres";
	
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
	
	private ExternalDataSource dataSource = null;
	
	private SyncThread syncThread;
	private int syncPeriod = 300; //seconds
	
	///Translation between scimma-admin's database representation of Kafka operations and the 
	///internal Kafka representation, since unfortunately they do not quite match. 
	private static HashMap<Integer, AclOperation> operationMap = new HashMap<Integer,AclOperation>();
	
	static {
		operationMap.put(1, AclOperation.ALL);
		operationMap.put(2, AclOperation.READ);
		operationMap.put(3, AclOperation.WRITE);
		operationMap.put(4, AclOperation.CREATE);
		operationMap.put(5, AclOperation.DELETE);
		operationMap.put(6, AclOperation.ALTER);
		operationMap.put(7, AclOperation.DESCRIBE);
		operationMap.put(8, AclOperation.CLUSTER_ACTION);
		operationMap.put(9, AclOperation.DESCRIBE_CONFIGS);
		operationMap.put(10, AclOperation.ALTER_CONFIGS);
		operationMap.put(11, AclOperation.IDEMPOTENT_WRITE);
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
		//if the action is a READ, and the subject is a public topic, we can authorize without needing to examine the principal
		if(isTopicPubliclyReadable(topic) && (action.operation()==AclOperation.READ || action.operation()==AclOperation.DESCRIBE)){
			LOG.info("ALLOWED "+messageBase+" due to the target being publicly readable");
			return AuthorizationResult.ALLOWED;
		}
		if(topic.equals("__consumer_offsets")){
			LOG.info("ALLOWED "+messageBase+" by hard-coded rule");
			return AuthorizationResult.ALLOWED;
		}
		
		String username=requestContext.principal().getName();
		HashSet<AclBinding> userPerms=ACLs.get(username);
		if(userPerms==null){
			//try to fetch directly from the database
			try(ConnectionWrapper conn=dataSource.getConnection()){
				gatherPermissions(conn, username);
			}
			catch(SQLException ex){
				LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
				LOG.info("DENIED "+messageBase+" due to lack of permission data");
				return AuthorizationResult.DENIED;
			}
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
		AccessControlEntry entryExact = new AccessControlEntry("User:"+username,"*",action.operation(),AclPermissionType.ALLOW);
		if(userPerms.contains(new AclBinding(pattern, entryExact))){
			LOG.info("ALLOWED "+messageBase+" due to an exact permission match");
			return AuthorizationResult.ALLOWED;
		}
		AccessControlEntry entryGeneral = new AccessControlEntry("User:"+username,"*",AclOperation.ALL,AclPermissionType.ALLOW);
		if(userPerms.contains(new AclBinding(pattern, entryGeneral))){
			LOG.info("ALLOWED "+messageBase+" due to a match with an ALL permission rule");
			return AuthorizationResult.ALLOWED;
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
			checkPublicTopic(topic);
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
		try(ConnectionWrapper conn=dataSource.getConnection()){
			gatherPermissions(conn);
			checkPublicTopics(conn, null);
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
		}
	}
	
	/**
	 Update all cached user permission data. This will load all data, whether or not it has been 
	 previously used locally (permissions are fetched for all users). If communication with database 
	 fails, old cached data is retained.   
	 */
	private void gatherPermissions(ConnectionWrapper conn){
		gatherPermissions(conn, null);
	}
	
	/**
	 Update cached user permission data. If communication with database fails, old cached data is
	 retained.
	 @param specificUser A single user for which to look up permissions. 
	                     If null, all user permissions will be loaded. 
	 */
	private void gatherPermissions(ConnectionWrapper conn, String specificUser){
		String query="SELECT hopskotch_auth_credentialkafkapermission.operation,hopskotch_auth_scramcredentials.username,hopskotch_auth_kafkatopic.name "
			+"FROM hopskotch_auth_credentialkafkapermission "
			+"INNER JOIN hopskotch_auth_scramcredentials ON hopskotch_auth_credentialkafkapermission.principal_id=hopskotch_auth_scramcredentials.id "
			+"INNER JOIN hopskotch_auth_kafkatopic ON hopskotch_auth_credentialkafkapermission.topic_id=hopskotch_auth_kafkatopic.id ";
		if(specificUser!=null){
            query+=" WHERE hopskotch_auth_scramcredentials.username=?";
			LOG.debug("Querying database for permissions for user "+specificUser);
		}
		try(PreparedStatement st = conn.getConnection().prepareStatement(query);){
			if(specificUser!=null)
				st.setString(1, specificUser);
			
			try(ResultSet rs = st.executeQuery()){
				//make a new global map only if querying for all users
				ConcurrentHashMap<String, HashSet<AclBinding>> updatedACLs=null;
				//if querying for a single user, make just a new set of permissions for that user
				HashSet<AclBinding> userPerms=null;
				if(specificUser!=null)
					userPerms=new HashSet<AclBinding>();
				else
					updatedACLs=new ConcurrentHashMap<String, HashSet<AclBinding>>();
				
				while(rs.next()){
					String username = "User:"+rs.getString("username");
					String topic = rs.getString("name");
					int operation = rs.getInt("operation");
					AclOperation kafkaOperation = operationMap.get(operation);
					LOG.debug("Found permission for user "+username+" to perform operation "+kafkaOperation.toString()+" on topic "+topic);
					ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
					AccessControlEntry entry = new AccessControlEntry(username,"*",kafkaOperation,AclPermissionType.ALLOW);
					
					//if gathering data for all users, insert into the new global map
					//note that we continue to check user rather than userPerms in order to do the 
					//right thing on subsequent iterations
					if(specificUser==null){
						userPerms=updatedACLs.get(username);
						if(userPerms==null){
							userPerms = new HashSet<AclBinding>();
							updatedACLs.put(username, userPerms);
						}
					}
					userPerms.add(new AclBinding(pattern, entry));
					
					if(kafkaOperation!=AclOperation.ALL && kafkaOperation!=AclOperation.DESCRIBE){
						LOG.debug("Added implicit permission for user "+username+" to DESCRIBE on topic "+topic);
						entry = new AccessControlEntry(username,"*",AclOperation.DESCRIBE,AclPermissionType.ALLOW);
						userPerms.add(new AclBinding(pattern, entry));
					}
				}
				
				if(specificUser!=null) //if updating a single user, overwrite just that entry in the global map
					ACLs.put(specificUser, userPerms);
				else  //replace the whole global map
					ACLs = updatedACLs;
			}
			catch(SQLException ex){
				LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
				dataSource.markConnectionBad();
				return;
			}
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
			dataSource.markConnectionBad();
			return;
		}
	}
	
	/**
	 Update cached public topic setting data. If communication with database fails, old cached data
	 is retained.
	 @param specificTopic A single topic for which to look up the setting. 
	 */
	private void checkPublicTopic(String specificTopic){
		LOG.debug("Querying database for public status of "+specificTopic);
		try(ConnectionWrapper conn=dataSource.getConnection()){
			checkPublicTopics(conn, specificTopic);
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
		}
	}
	
	/**
	 Update cached public topic setting data. If communication with database fails, old cached data
	 is retained.
	 @param specificTopic A single topic for which to look up the setting. 
	                      If null, all topic settings will be looked up. 
	 */
	private void checkPublicTopics(ConnectionWrapper conn, String specificTopic){
		String query="SELECT name,publicly_readable FROM hopskotch_auth_kafkatopic";
		if(specificTopic!=null)
			query+=" WHERE name = ?";
		try(PreparedStatement st = conn.getConnection().prepareStatement(query);){
			if(specificTopic!=null)
				st.setString(1, specificTopic);
			
			try(ResultSet rs = st.executeQuery()){
				ConcurrentHashMap<String, Boolean> updatedPublicTopics;
				if(specificTopic==null) //create a new Map
					updatedPublicTopics = new ConcurrentHashMap<String, Boolean>();
				else //otherwise update the existing Map
					updatedPublicTopics = publicTopics;
				while(rs.next()){
					String topic = rs.getString("name");
					boolean publiclyReadable = rs.getBoolean("publicly_readable");
					updatedPublicTopics.put(topic, publiclyReadable);
					if(publiclyReadable)
						LOG.debug("Topic "+topic+" is publicly readable");
					else
						LOG.debug("Topic "+topic+" is not publicly readable");
				}
				publicTopics = updatedPublicTopics;
			}
			catch(SQLException ex){
				LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
				dataSource.markConnectionBad();
				return;
			}
		}
		catch(SQLException ex){
			LOG.warn("Failed to connect to database, aborting sync.\nSQL Error: "+ex.getMessage());
			dataSource.markConnectionBad();
			return;
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
		if(dataSource!=null)
			dataSource.close();
	}
}
