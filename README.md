# MavenPop

Big Data application to determine Maven artifacts popularity taking into account transitive dependencies

This is an experiment at an early stage.


## System Overview

Maven is used widely in packaging and distribution of many open source projects and Maven repository server logs can be used to extract valuable information about the community adoption of new software releases.

Maven is able to resolve transitive dependencies and, therefore, performing simple counting techniques does not provide insight on direct user choices, since an artifact could be popular because it is a common (transitive) dependency of other artifacts. The aim of this project is to take into consideration transitive dependencies to calculate artifact popularity, differentiating “direct usages” or top level dependencies from the “indirect usages” or transitive dependencies.

The following image is an overview of the system:

![System Overview](SystemOverview.png)

MavenPop is a Big Data Analytics application that receives “Dependency Records” and “Repository Logs” and generates popularity information on direct and indirect usage of the artifacts.

Dependency Records contain Maven repository coordinates of the artifacts along with a list of the dependencies of each artifact. The dependencies are also in Maven coordinates format. For the cases the dependencies of a given artifact are not known, MavenPop expects a special value instead of the list of dependencies. Repository Logs comprise a client identifier (generated from anonymized IP addresses for example), artifact path and a timestamp indicating the exact time the client retrieved the given artifact.

MavenPop uses the dependency records to build a dependency graph of all known artifacts. It builds sessions of consecutive retrievals from the repository logs and use the dependency graph to determine which artifacts from each session are direct choice from the users. Direct and indirect choices are aggregated in users per day and saved in a comma-separated-values files corresponding to “Direct Usage” and “Indirect Usage”.

It is possible to specify the maximum idle time and the report date.
Maximum idle time represents the maximum time a user can spend between two retrievals for them to be considered part of the same session.

## Data Representation

### Dependency Records

MavenPop expects the dependency records in the following format:
```bash
{occurrences, relative path, GAV coordinates, dependencies}
```
Where occurences is the number of times the artifact appears in the repository logs,
relative path is the path to the artifact,
GAV coordinates is the artifact identifier in the maven repositories,
and dependencies is the list of GAVs that the artifact depends on.
The 'dependencies' field can also adopt the special values
UNKNOWN_DEPS (there is no known dependencies for this artifact )
or (NO_DEPS) when it is known that the artifact does not have any dependency

For example:
```bash
3221 rome/rome/1.0/rome-1.0.pom rome:rome:1.0 jdom:jdom:1.0,junit:junit:3.8.2
325 com/c2/fit/fit/1.1/fit-1.1.pom com.c2.fit:fit:1.1 junit:junit:3.8.1
1645 jboss/jbpm/3.1.1/jbpm-3.1.1.pom jboss:jbpm:3.1.1 NO_DEPS
2329145 org/codehaus/mojo/maven-metadata.xml org.codehaus:mojo UNKNOWN_DEPS
```

### Repository Logs

MavenPop expects the repository logs in the following format:

```bash
{client ID, timestamp, agent, artifact}
```
Where client ID is the identifier generated for a client based on its IP address,
timestamp is the number of milliseconds ellapsed since 1st January 1970,
agent is the connector the client is using to access the repository and
artifact is the software component requested

For example:
```bash
427 1433855407000 m2e junit/junit/3.8/junit-3.8.pom
673 1403637289000 Maven javax/mail/mail/1.4/mail-1.4-sources.jar
2530 1274090245000 Ivy org/jboss/jboss-parent/3/jboss-parent-3.pom
839 1399622120000 Nexus org/codehaus/mojo/maven-metadata.xml
```

### Output Report

MavenPop generates 2 CSV files per report date with the name <report_date>\_direct.csv and <report_date>\_indirect.csv.
Each file contains the aggregations of artifacts direct (top level dependency in session) and indirect (transitive dependency in session) usage.
report\_date is in the format YYYY-MM-DD. Both files have data in the format:

```bash
{artifact, users_count}
```
Where artifact is the maven coordinates of the software component retrieved,
and users_count is the amount of distinct users that retrieved this artifact.

Example:

- filename: 2018-05-13_direct.csv
```bash
org.apache.maven.plugins:maven-install-plugin,3
org.jvnet.staxex:stax-ex,4
```

- filename: 2018-05-13_indirect.csv
```bash
org.drools:knowledge-internal-api:5.4.0.Final,1
org.drools:drools-core:5.4.0.Final,1
```

## Usage

### Prerrequisites

#### Spark

#### Neo4j

Download Neo4j Server 3.4.1 community eddition from https://neo4j.com/download/other-releases/#releases
and Follow the installation instructions

Save NEO4J_HOME in an enviroment variable

NEO4J_HOME=/path/to/extracted/neo4j-community-3.4.1





