ifeq ($(filter -r,$(MAKEFLAGS)),)
MAKEFLAGS += -r
endif

PLUGIN_VERSION=0.2.0

KAFKA_VERSION=3.9.2
KAFKA_TARBALL=kafka_2.13-$(KAFKA_VERSION).tgz
KAFKA_DOWNLOAD_URL=https://archive.apache.org/dist/kafka/$(KAFKA_VERSION)/$(KAFKA_TARBALL)
KAFKA_JAR=kafka_2.13-$(KAFKA_VERSION)/libs/kafka-clients-$(KAFKA_VERSION).jar

JOSE4J_VERSION=0.9.6
# bundled with Kafka, no distinct download URL or tarball name
JOSE4J_JAR=kafka_2.13-$(KAFKA_VERSION)/libs/jose4j-$(JOSE4J_VERSION).jar

SLF4J_VERSION=2.0.18
SLF4J_JAR=slf4j-api-$(SLF4J_VERSION).jar
SLF4J_DOWNLOAD_URL=https://repo1.maven.org/maven2/org/slf4j/slf4j-api/$(SLF4J_VERSION)/slf4j-api-$(SLF4J_VERSION).jar

JSON_VERSION=20260522
JSON_JAR=json-$(JSON_VERSION).jar
JSON_DOWNLOAD_URL=https://repo1.maven.org/maven2/org/json/json/$(JSON_VERSION)/json-$(JSON_VERSION).jar

NIMBUS_VERSION=10.8
NIMBUS_JAR=com.nimbusds.nimbus-jose-jwt-$(NIMBUS_VERSION).jar
NIMBUS_DOWNLOAD_URL=https://repo1.maven.org/maven2/com/nimbusds/nimbus-jose-jwt/$(NIMBUS_VERSION)/nimbus-jose-jwt-$(NIMBUS_VERSION).jar

DEPENDENCIES=deps/$(KAFKA_JAR) deps/$(SLF4J_JAR) deps/$(JSON_JAR) deps/$(JOSE4J_JAR)

BUILD_CLASSPATH=.:deps/$(KAFKA_JAR):deps/$(SLF4J_JAR):deps/$(JOSE4J_JAR):deps/$(JSON_JAR)

JAVAC_FLAGS=-Xlint:deprecation -Xlint:unchecked
#-Xlint:all


CLASSES=build/scimma/LockGuard.class build/scimma/RestClient.class build/scimma/ExternalScramAuthnCallbackHandler.class build/scimma/ExternalAuthorizer.class \
        build/scimma/TokenAuthnCallbackHandler.class


all : build/ScimmaAuthPlugin.jar

build : 
	mkdir build

deps/$(KAFKA_TARBALL) :
	mkdir -p deps && cd deps && curl -LO --fail $(KAFKA_DOWNLOAD_URL)

deps/$(KAFKA_JAR) : deps/$(KAFKA_TARBALL)
	tar xzf deps/$(KAFKA_TARBALL) -C deps $(KAFKA_JAR)
	test -f deps/$(KAFKA_JAR) && touch deps/$(KAFKA_JAR)

deps/$(JOSE4J_JAR) : deps/$(KAFKA_TARBALL)
	tar xzf deps/$(KAFKA_TARBALL) -C deps $(JOSE4J_JAR)
	test -f deps/$(JOSE4J_JAR) && touch deps/$(JOSE4J_JAR)

deps/$(SLF4J_JAR) :
	mkdir -p deps && cd deps && curl -L --fail $(SLF4J_DOWNLOAD_URL) -o $(SLF4J_JAR)

deps/$(JSON_JAR) :
	mkdir -p deps && cd deps && curl -L --fail $(JSON_DOWNLOAD_URL) -o $(JSON_JAR)


build/ScimmaAuthPlugin.jar : build $(CLASSES)
	cd build && jar cf ScimmaAuthPlugin.jar scimma

build/scimma/LockGuard.class : scimma/LockGuard.java $(DEPENDENCIES)
	CLASSPATH=$(BUILD_CLASSPATH) javac $(JAVAC_FLAGS) scimma/LockGuard.java -d build

build/scimma/RestClient.class : scimma/RestClient.java $(DEPENDENCIES)
	CLASSPATH=$(BUILD_CLASSPATH) javac $(JAVAC_FLAGS) scimma/RestClient.java -d build

build/scimma/ExternalScramAuthnCallbackHandler.class : scimma/ExternalScramAuthnCallbackHandler.java $(DEPENDENCIES)
	CLASSPATH=$(BUILD_CLASSPATH) javac $(JAVAC_FLAGS) scimma/ExternalScramAuthnCallbackHandler.java -d build

build/scimma/ExternalAuthorizer.class : scimma/ExternalAuthorizer.java $(DEPENDENCIES)
	CLASSPATH=$(BUILD_CLASSPATH) javac $(JAVAC_FLAGS) scimma/ExternalAuthorizer.java -d build

build/scimma/TokenAuthnCallbackHandler.class : scimma/TokenAuthnCallbackHandler.java $(DEPENDENCIES)
	CLASSPATH=$(BUILD_CLASSPATH) javac $(JAVAC_FLAGS) scimma/TokenAuthnCallbackHandler.java -d build


.SUFFIXES:

.PHONY : clean clean-deps test version

clean : 
	rm -rf build

clean-deps :
	rm -rf deps

version : 
	@echo "$(PLUGIN_VERSION)"

test : build/ScimmaAuthPlugin.jar
	CLASSPATH=$(CLASSPATH):build/ScimmaAuthPlugin.jar java scimma.ExternalScramAuthnCallbackHandler
