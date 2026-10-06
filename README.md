# Requirements

- Java 7 or newer
- curl to download dependencies

## Java Dependencies

The following Java dependencies are required to build the plugin. 
They are automatically downloaded by the makefile, using pinned versions configured there as variables. 

- Apache Kafka
- Simple Logging Facade for Java (SLF4J, http://www.slf4j.org)
- JSON in Java (https://github.com/stleary/JSON-java)

Of these, the JSON jar must also generally be added to the runtime environment, as it is not a standard Kafka dependency. 

The `make clean-deps` command can be used to remove downloaded dependencies. 

# Compilation

To build or rebuild the plugin, it should only be necessary to run `make` in the project directory. 

# Configuration

After compiling the plugin, the resulting `ScimmaAuthPlugin.jar` (placed in the `build` subdirectory) must be added to the `CLASSPATH` to be found by Kafka. The JSON jar file should also be added. 

## Kafka settings

To instruct Kafka to use this plugin for authentication lookups configure

	listener.name.sasl_$(PROTOCOL).$(MECHANISM).sasl.server.callback.handler.class=ExternalScramAuthnCallbackHandler

in your server properties configuration file. 
For example, to use this plugin for the plaintext protocol and the SHA-512 SCRAM mechanism, configure:

	listener.name.sasl_plaintext.scram-sha-512.sasl.server.callback.handler.class=scimma.ExternalScramAuthnCallbackHandler

All Kafka configuration settings for this plugin are prefixed by `ExternalScramAuthnCallbackHandler`. 
They include:

- `ExternalScramAuthnCallbackHandler.apiRoot` - The URL for the scimma-admin/hopauth REST API.
- `ExternalScramAuthnCallbackHandler.apiUsername` - The username for authenticating with hopauth.
- `ExternalScramAuthnCallbackHandler.apiPassword` - The password for authenticating with hopauth.
- `ExternalScramAuthnCallbackHandler.syncPeriod` - The length of time to wait between full synchronizations with the hopauth API, in seconds. Defaults to 300 (seconds).

To configure use of the authorization portion of the plugin, add:

	authorizer.class.name=scimma.ExternalAuthorizer

The settings for the authorizer are analogous to the authenticator:

- `ExternalAuthorizer.apiRoot` - The URL for the scimma-admin/hopauth REST API.
- `ExternalAuthorizer.apiUsername` - The username for authenticating with hopauth.
- `ExternalAuthorizer.apiPassword` - The password for authenticating with hopauth.
- `ExternalAuthorizer.syncPeriod` - The length of time to wait between full synchronizations with the hopauth API, in seconds. Defaults to 300 (seconds).


This plugin also contains an alternative authentication mechanism, using JWTs, which wotks with the built-in OAuthBearerLoginModule. 
It can be configured for a chosen listener via:

	listener.name.$(LISTENER).oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
	listener.name.$(LISTENER).oauthbearer.sasl.server.callback.handler.class=scimma.TokenAuthnCallbackHandler

The settings for this class are:

- `TokenAuthnCallbackHandler.trusted.issuers` - A comma separated list of issuers which should be trusted for authentication of users.
- `TokenAuthnCallbackHandler.sub.claim.name` - The name of the token claim to use as the initial subject identifier.
- `TokenAuthnCallbackHandler.source.property` - The name of the hopauth property to use as the source for mapping users, defaults to `email`.
- `TokenAuthnCallbackHandler.target.property` - The name of the hopauth property to use as the target for mapping users, defaults to `username`.
- `TokenAuthnCallbackHandler.jwks.cache.ttl.seconds` - The length of time to cache JWKS data from issuers.
- `TokenAuthnCallbackHandler.jwks.refresh.interval.seconds` - The minimum time period to repeat requesting JWKS data from issuers.
- `TokenAuthnCallbackHandler.apiRoot` - The URL for the scimma-admin/hopauth REST API.
- `TokenAuthnCallbackHandler.apiUsername` - The username for authenticating with hopauth.
- `TokenAuthnCallbackHandler.apiPassword` - The password for authenticating with hopauth.
- `TokenAuthnCallbackHandler.syncPeriod` - The length of time to wait between full synchronizations with the hopauth API, in seconds. Defaults to 300 (seconds).


## Logging configuration

Logging verbosity can be controlled by setting the following properties in `log4j.properties`:

- `log4j.logger.scimma.ExternalScramAuthnCallbackHandler.logger`  - Logging for SCRAM authentication
- `log4j.logger.scimma.ExternalAuthorizer.logger`  - Logging for authorization; at least INFO level is recommended
- `log4j.logger.scimma.TokenAuthnCallbackHandler.logger`  - Logging for token authentication
- `log4j.logger.scimma.RestClient.logger`  - Logging for low-level REST connection details
