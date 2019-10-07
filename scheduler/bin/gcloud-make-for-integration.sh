##
## Configure a kubernetes cluster for running pool-based integration tests and running pools in general.
##
##


set -e

## Example arguments that work:
##

#ZONE=us-central1-a
#CLUSTERNAME=test-cluster-2
#PROJECT=<FILL SOMETHING IN>

PROJECT=$1
ZONE=$2
CLUSTERNAME=$3

echo "---- Building kubernetes cluster for project=$PROJECT zone=$ZONE clustername=$CLUSTERNAME"

# Nuke all existing temporary clusters; don't want to keep on making more idle clusters each time you invoke this.
echo "---- Deleting any existing temporary clusters."
echo "---- Will delete the following. If you don't want them gone, control-c now"
gcloud container clusters list --filter 'resourceLabels.longevity=temporary'
sleep 10
for i in `gcloud container clusters list --filter 'resourceLabels.longevity=temporary' --format="value(name)"` ; do gcloud --quiet container clusters delete $i ; done

# This creates a cluster, with some nodes that are needed for kubernetes management.
# Create 7 nodes, with three tainted for alpha, 3 untainted and 3 tainted with gamma pool.
# The second line of flags about upgrades and legacy endpoints is to suppress some warnings.
echo "---- Creating new cluster (please wait 5 minutes)"
time gcloud container clusters create $CLUSTERNAME --disk-size=20gb --machine-type=g1-small --num-nodes=3 --preemptible  \
     --enable-autoupgrade --no-enable-basic-auth --no-issue-client-certificate --no-enable-ip-alias --metadata disable-legacy-endpoints=true \
     --labels longevity=temporary
## The above takes like 3-4 mintues to run.

echo "---- Setting up gcloud credentials"
gcloud container clusters get-credentials $CLUSTERNAME --region $ZONE

# Make some node-pools --- this takes a while, but it helps guarantee that we have nodes with the appropriate cook.pool taints.
echo "---- Making extra alpha and gamma nodepools for cook pools (please wait 5-10 minutes)"
gcloud container node-pools create cook-pool-gamma --cluster=$CLUSTERNAME --disk-size=20gb --machine-type=g1-small \
       --node-taints=cook.pool=gamma:NoSchedule \
       --num-nodes=2
gcloud container node-pools create cook-pool-alpha --cluster=$CLUSTERNAME --disk-size=20gb --machine-type=g1-small \
       --node-taints=cook.pool=alpha:NoSchedule \
       --num-nodes=3

echo "---- Setting up cook namespace in kubernetes"
kubectl create -f docs/make-kubernetes-namespace.json 

echo "---- Showing the clusters and nodes we generated"
gcloud container clusters list
kubectl get nodes

