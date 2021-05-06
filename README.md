# Requirements

- Java 7 or newer
- curl to download dependencies

## Java Dependencies

The following Java dependencies are required to build the plugin. 
They are automatically downloaded by the makefile, using pinned versions configured there as variables. 

- Apache Kafka
- Simple Logging Facade for Java (SLF4J, http://www.slf4j.org)
- PostgreSQL JDBC Driver (https://jdbc.postgresql.org)

Of these, the JDBC Driver must also generally be added to the runtime environment, as it is not a standard kafka dependency. 

The `make clean-deps` command can be used to remove downloaded dependencies. 

# Compilation

To build or rebuild the plugin, it should only be necessary to run `make` in the project directory. 

# Configuration

After compiling the plugin, the resulting `ScimmaAuthPlugin.jar` (placed in the `build` subdirectory) must be added to the `CLASSPATH` to be found by Kafka. The PostgreSQL JDBC jar file should also be added. 

## Kafka settings

To instruct Kafka to use this plugin for authentication lookups configure

	listener.name.sasl_$(PROTOCOL).$(MECHANISM).sasl.server.callback.handler.class=ExternalScramAuthnCallbackHandler

in your server properties configuration file. 
For example, to use this plugin for the plaintext protocol and the SHA-512 SCRAM mechanism, configure:

	listener.name.sasl_plaintext.scram-sha-512.sasl.server.callback.handler.class=ExternalScramAuthnCallbackHandler

All Kafka configuration settings for this plugin are prefixed by `ExternalScramAuthnCallbackHandler`. 
They include:

- `ExternalScramAuthnCallbackHandler.postgres.host` - The PostgreSQL server. Defaults to 'localhost'.
- `ExternalScramAuthnCallbackHandler.postgres.database` - The name of the PostgreSQL database to which to connect. Defaults to 'scimma'
- `ExternalScramAuthnCallbackHandler.postgres.user` - The name of the PostgreSQL username to use. Defaults to 'scimma_user'. 
- `ExternalScramAuthnCallbackHandler.postgres.password` - The name of the PostgreSQL password to use. This setting has no default, and must always be specified. 
- `ExternalScramAuthnCallbackHandler.postgres.ssl` - Whether to use SSL transport security when communicating with the database. Currently defaults to false. 
- `ExternalScramAuthnCallbackHandler.syncPeriod` - The length of time to wait between full synchronizations with the database, in seconds. Defaults to 300 (seconds). 

## Logging configuration

The logging level used by this plugin is controlled by the `log4j.logger.ExternalScramAuthnCallbackHandler.logger` setting in `log4j.properties`. 