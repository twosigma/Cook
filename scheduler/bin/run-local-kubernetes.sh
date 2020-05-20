#!/usr/bin/env bash

# Usage: ./bin/run-local-kubernetes.sh
# Runs the cook scheduler locally.

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCHEDULER_DIR="$( dirname "${DIR}" )"

# Defaults (overridable via environment)
: ${COOK_DATOMIC_URI="datomic:mem://cook-jobs"}
: ${COOK_FRAMEWORK_ID:=cook-framework-$(date +%s)}
: ${COOK_KEYSTORE_PATH:="${SCHEDULER_DIR}/cook.p12"}
: ${COOK_NREPL_PORT:=${2:-8888}}
: ${COOK_PORT:=${1:-12321}}
: ${COOK_SSL_PORT:=${3:-12322}}
: ${MASTER_IP:="127.0.0.2"}
: ${ZOOKEEPER_IP:="127.0.0.1"}
: ${MESOS_NATIVE_JAVA_LIBRARY:="/usr/local/lib/libmesos.dylib"}


if [ "${COOK_ZOOKEEPER_LOCAL}" = false ] ; then
    COOK_ZOOKEEPER="${ZOOKEEPER_IP}:2181"
    echo "Cook ZooKeeper configured to ${COOK_ZOOKEEPER}"
else
    COOK_ZOOKEEPER=""
    COOK_ZOOKEEPER_LOCAL=true
    echo "Cook will use local ZooKeeper"
fi

if [ ! -f "${COOK_KEYSTORE_PATH}" ];
then
    keytool -genkeypair -keystore "${COOK_KEYSTORE_PATH}" -storetype PKCS12 -storepass cookstore -dname "CN=cook, OU=Cook Developers, O=Two Sigma Investments, L=New York, ST=New York, C=US" -keyalg RSA -keysize 2048
fi

echo "Creating environment variables..."
export COOK_DATOMIC_URI="${COOK_DATOMIC_URI}"
export COOK_FRAMEWORK_ID="${COOK_FRAMEWORK_ID}"
export COOK_ONE_USER_AUTH=$(whoami)
export COOK_HOSTNAME="cook-scheduler-${COOK_PORT}"
export COOK_LOG_FILE="log/cook-${COOK_PORT}.log"
export COOK_NREPL_PORT="${COOK_NREPL_PORT}"
export COOK_PORT="${COOK_PORT}"
export COOK_ZOOKEEPER="${COOK_ZOOKEEPER}"
export COOK_ZOOKEEPER_LOCAL="${COOK_ZOOKEEPER_LOCAL}"
export LIBPROCESS_IP="${MASTER_IP}"
export MESOS_MASTER="${MASTER_IP}:5050"
export MESOS_NATIVE_JAVA_LIBRARY="${MESOS_NATIVE_JAVA_LIBRARY}"
export COOK_SSL_PORT="${COOK_SSL_PORT}"
export COOK_KEYSTORE_PATH="${COOK_KEYSTORE_PATH}"

echo "Getting GKE credentials..."
filter="resourceLabels.longevity=temporary AND resourceLabels.owner=$GKE_CLUSTER_OWNER"
gcloud container clusters list --filter "$filter"
i=1
for cluster_zone in $(gcloud container clusters list --filter "$filter" --format="csv(name,zone)" | tail -n +2)
do
    cluster=$(echo "$cluster_zone" | cut -d',' -f1)
    zone=$(echo "$cluster_zone" | cut -d',' -f2)
    echo "Getting credentials for cluster $cluster in zone $zone ($i)"
    KUBECONFIG=.cook_kubeconfig_$i gcloud container clusters get-credentials "$cluster" --zone "$zone"
    ((i++))
done
KUBECONFIG=.cook_kubeconfig_1 kubectl get pods --namespace cook
KUBECONFIG=.cook_kubeconfig_2 kubectl get pods --namespace cook

echo "Starting cook..."
rm -f "$COOK_LOG_FILE"
lein run config-k8s.edn
