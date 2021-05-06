KAFKA_VERSION=2.6.2
KAFKA_SRC=kafka-$(KAFKA_VERSION)-src
KAFKA_TARBALL_SUFFIX=.tgz
KAFKA_DOWNLOAD_URL=https://downloads.apache.org/kafka/$(KAFKA_VERSION)/$(KAFKA_SRC)$(KAFKA_TARBALL_SUFFIX)

POSTGRES_JDBC_VERSION=42.2.19
POSTGRES_JAR=postgresql-$(POSTGRES_JDBC_VERSION).jar
POSTGRES_DOWNLOAD_URL=https://jdbc.postgresql.org/download/$(POSTGRES_JAR)

SLF4J_VERSION=2.0.0-alpha1
SLF4J_JAR=org.slf4j.jar
SLF4J_DOWNLOAD_URL=https://repo1.maven.org/maven2/org/slf4j/slf4j-api/$(SLF4J_VERSION)/slf4j-api-$(SLF4J_VERSION).jar

DEPENDENCIES=deps/$(POSTGRES_JAR) deps/$(KAFKA_SRC) deps/org.slf4j.jar

BUILD_CLASSPATH=deps/$(SLF4J_JAR):deps/$(KAFKA_SRC)/clients/src/main/java/

all : build/ScimmaAuthPlugin.jar

build : 
	mkdir build

deps : 
	mkdir deps

deps/$(POSTGRES_JAR) : deps
	cd deps && curl -LO $(POSTGRES_DOWNLOAD_URL)

deps/$(KAFKA_SRC) : deps/$(KAFKA_SRC)$(KAFKA_TARBALL_SUFFIX)
	tar xzf deps/$(KAFKA_SRC)$(KAFKA_TARBALL_SUFFIX) -C deps

deps/$(KAFKA_SRC)$(KAFKA_TARBALL_SUFFIX) : deps
	cd deps && curl -LO $(KAFKA_DOWNLOAD_URL)

deps/org.slf4j.jar : 
	cd deps && curl -L $(SLF4J_DOWNLOAD_URL) -o $(SLF4J_JAR)

build/ScimmaAuthPlugin.jar : build build/ExternalScramAuthnCallbackHandler.class
	cd build && jar cf ScimmaAuthPlugin.jar *.class

build/ExternalScramAuthnCallbackHandler.class : ExternalScramAuthnCallbackHandler.java $(DEPENDENCIES)
	CLASSPATH=$(BUILD_CLASSPATH) javac ExternalScramAuthnCallbackHandler.java -d build

.PHONY : clean clean-deps test

clean : 
	rm -rf build

clean-deps :
	rm -rf deps

test : build/ScimmaAuthPlugin.jar deps/$(POSTGRES_JAR)
	CLASSPATH=$(CLASSPATH):deps/$(POSTGRES_JAR):build java ExternalScramAuthnCallbackHandler