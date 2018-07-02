# MavenPop Dependency Loader

This repository contains the tools and configuration used to populate and deploy the graph database of the "MavenPop" project.

The dependency records are expected in the following format:
  {count path gav depencencies}
Where dependencies is a comma-sepparated string of gavs that dependens on 'gav'.

## Generate and verify the graph database file

1. Compile the data parser:
```bash
scalac neoDataParser.scala -d parser.jar
```
2. Set the NEO4J_HOME environment variable to the home of neo4j
```bash
export NEO4J_HOME=/path/to/neo4j-community-3.3.5/
```
3. Import the dependencies into a graph database
```bash
./import_and_start_neo.bash /path/to/dependency/file graph.db
```
The above command will generate the dependency graph database and start neo4j

4. Validate the database is up and running with Neo4j Browser and stop neo4j:
```bash
$NEO4J_HOME/neo4j stop
```
The generated database is on $NEO4J_HOME/data/databases/graph.db/

5. Make a tar file of the database
```bash
cd $NEO4J_HOME/data/databases/
tar cfz graph.db
```

## Make the Generated Database available for Neo4J pods:

Here we configure a persistent volume on minishift and copy the generated database to the databases directory, so when the neo4j container start they already have the dependency information available.

### Grant host access to user 'developer'

```bash
oc login -u system:admin
oc adm policy add-scc-to-user hostaccess developer
```

### Configure Minishift internal directory for persistent volume
Note: The GID and STORAGE_RESOURCE here must match the GID parameter for neo4j-local-pv.yaml
```bash
minishift ssh

sudo -i
STORAGE_RES=/var/lib/minishift/openshift.local.pv/pv-neo4j
GID=2018

mkdir -p $STORAGE_RES
chown root:$GID $STORAGE_RES
chmod 770 $STORAGE_RES
```
Exit minishift VM.

### Create Persistent Volume
Note: Ensure the GID parameter matches the GID specified in section while configuring internal directory
```bash
oc login -u system:admin

oc process -f neo4j-local-pv.yaml \
-p PV_NAME=pv-neo4j \
-p GID=2018 \
-p DIR=/var/lib/minishift/openshift.local.pv/pv-neo4j \
| oc create -f -

oc get pv pv-neo4j

oc login -u developer
```

### Add host folder to minishift and copy pre-populated dependency database

Make local directory and share it with minishift VM
```bash
mkdir -p /path/to/share/directory/in/host
minishift hostfolder add -t sshfs --source /path/to/share/directory/in/host --target /mnt/sda1/host-share neo-share
cp /$NEO4J_HOME/data/databases/graph.db.tar.gz /path/to/share/directory/in/host
```
Verify the database is in minishift
```bash
minishift hostfolder list
minishift ssh "ls -l /mnt/sda1/host-share"
```
Copy the database to neo4j database directory with proper permissions
```bash
minishift ssh
sudo su -
cd /var/lib/minishift/openshift.local.pv/pv-neo4j/databases/

USERID=`ls -l | awk '{if(NR>1)print $3}'`
GROUPID=`ls -l | awk '{if(NR>1)print $4}'`

rm -rf graph.db
tar xfz /mnt/sda1/host-share/graph.db.tar.gz

chown -R $USERID:$GROUPID graph.db/
```
Exit minishift VM

### Create Deployment Configuration

Create persistent volume claim and deployment configuration

```bash
oc login -u developer
oc project neo4j

oc process -f neo4j-dc-single.yaml \
-p PV_NAME=pv-neo4j \
-p PVC_NAME=neo4j-storage \
| oc create -f -

oc get all
oc get pvc
```

### Port forward

Enable communication with Neo4J from outside openshift.
This is a provisional solution, since at a latter stage the spark cluster is intendended to run inside openshift, thus no ingress would be necessary.
```bash
oc get pod -o=go-template --template '{{range .items}}{{.metadata.name}}{{end}}'
oc get pod

oc port-forward <pod_id> 17687:7687

oc get all
```
Note the Neo4j service is running on bolt://localhost:17687
