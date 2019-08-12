# Configuring kubernetes
There are two main ways to configure the connection to the kubernetes cluster:
## Config file
One option is to specify the path to a kubernetes config YAML file, similar to one used by kubectl. Example:
```clojure    
:compute-clusters [{:factory-fn cook.kubernetes.compute-cluster/factory-fn
                    :config {:compute-cluster-name "minikube"
                             ;; Location of the kubernetes config file, e.g. $HOME/.kube/config
                             :config-file "/home/myuser/.kube/config.yml"}}]
```
## GKE cluster config
For GKE clusters, OAuth is used to authenticate with the cluster. To connect to a GKE cluster, you need two things:
- Google JSON credential file: https://cloud.google.com/iam/docs/creating-managing-service-account-keys
- Cluster master url (this will be the api client base URL)
Example:
```clojure
:compute-clusters [{:factory-fn cook.kubernetes.compute-cluster/factory-fn
                    :config {:base-url "http://127.0.0.1:8000"
                             ;; Location of credential file
                             :google-credentials "/home/myuser/creds.json"}}]
```

# Running with kubernetes has several differences:
- Use config-k8s.edn instead of config.edn
- You need to set a path to the kube config with COOK_K8S_CONFIG_FILE

These are done for you in bin/run-kubernetes-local.sh


You'll need to setup several vitual environments to use run-local.sh and run-kubernetes-local.sh.
* One for building the CLI.
* One for running run-kubernetes-local.sh
* One for integration tests

# Setup. Detailed command lines.

- Set up minikube

# Run local tidbits

- Start datomic:

scheduler/bin/start-datomic.sh

- Seed pools

lein exec -p scheduler/datomic/data/seed_pools.clj datomic:free://localhost:4334/jobs

- Make namespace

kubectl create -f docs/make-kubernetes-namespace.json

- Run local. Its OK to free and restart.

COOK_DATOMIC_URI=datomic:free://localhost:4334/jobs scheduler/bin/run-kubernetes-local.sh

# Tools and help

- Get a kubernetes dashboard:
minikube dashboard

- Submit a test job

cs submit -i alpine:latest -c .01 -m 16 "echo 200" 
