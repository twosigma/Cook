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
- Cluster master url (this will be the api client base URL). You can get this from 'gcloud container clusters list' or 'kubectl cluster-info'
Example:
```clojure
:compute-clusters [{:factory-fn cook.kubernetes.compute-cluster/factory-fn
                    :config {:compute-cluster-name "gke"
                             :base-url "http://<IP ADDRESS>:8000"
                             ;; Location of credential file
                             :google-credentials "/home/myuser/creds.json"}}]
```
## Additional configuration options
### `:namespace`
Cook has two ways of configuring the namespace kubernetes pods are launched in:
- Single static namespace: Cook can be configured to launch all kubernetes pods in a single static namespace. Example:
```clojure
:namespace {:kind :static
            :namespace "cook"}
```
- Per-user namespaces: Cook can also be configured to launch each pod in a namespace corresponding to the username of 
  the user who submitted the job. Example:
```clojure
:namespace {:kind :per-user}
```

# Running with kubernetes has several differences:
- Use config-k8s.edn instead of config.edn
- You need to set a path to the kube config with COOK_K8S_CONFIG_FILE

These are done for you in bin/run-kubernetes-local.sh


In order to run things, you'll need to setup several python virtual environments and pip install their requirements into them:
* One for building the CLI.
* One for running run-kubernetes-local.sh
* One for integration tests

# Setup. Detailed command lines.

- Set up minikube -- See https://kubernetes.io/docs/tasks/tools/install-minikube/

# Various setup needed to launch for the first time.

- Start datomic:

scheduler/bin/start-datomic.sh

- Seed pools -- Only needed if you use pools in config.edn. (Unnecessary with the config.edn used in this example.)

lein exec -p scheduler/datomic/data/seed_pools.clj datomic:free://localhost:4334/jobs

- Make namespace -- Needed the first time you use a kubernetes environment.

kubectl create -f docs/make-kubernetes-namespace.json

- Run local. It's OK to kill and restart.

COOK_DATOMIC_URI=datomic:free://localhost:4334/jobs scheduler/bin/run-kubernetes-local.sh

# Tools and help

- Get a kubernetes dashboard:
minikube dashboard

- Submit a test job

cs submit -i alpine:latest -c .01 -m 16 "echo 200" 

# Running integration tests
```bash
COOK_DEFAULT_JOB_CPUS=0.1 COOK_TEST_DOCKER_IMAGE=python:3.5 pytest -m 'not cli'
```
