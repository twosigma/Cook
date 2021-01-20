Vagrant.configure("2") do |config|
  config.vm.box = "hashicorp/bionic64"
  config.vm.network "forwarded_port", guest: 12321, host: 12321
  config.vm.provider "virtualbox" do |v|
    v.memory = 6144
    v.cpus = 2
  end

  # This runs as root:
  config.vm.provision :shell, path: "scheduler/bin/bootstrap", env: {"GKE_CLUSTER_OWNER" => ENV["USER"], "GCP_PROJECT_NAME" => ENV["GCP_PROJECT_NAME"]}

  # This runs as vagrant:
  $script = <<-SCRIPT
  repo_root=/vagrant
  bashrc=$HOME/.bashrc

  # Cook java jobclient setup
  cd $repo_root/jobclient/java || exit 1
  mvn install -DskipTests

  # Python setup
  pip3 install --upgrade pip
  pip3 install --upgrade setuptools
  pip3 install --upgrade wheel
  pip3 install --upgrade virtualenv
  cd $repo_root || exit 1
  venv=$repo_root/venv
  rm -rf $venv
  $HOME/.local/bin/virtualenv venv --python=python3.6
  source $venv/bin/activate
  echo "source $venv/bin/activate" | tee -a $bashrc
  export PATH=$venv/bin:$PATH
  echo 'export PATH='$venv'/bin:$PATH' | tee -a $bashrc

  # Integration tests setup
  echo "export COOK_TEST_DOCKER_IMAGE=gcr.io/google-containers/alpine-with-bash:1.0" | tee -a $bashrc
  echo "export COOK_TEST_DOCKER_WORKING_DIRECTORY=/mnt/sandbox" | tee -a $bashrc
  echo "export COOK_TEST_DISALLOW_POOLS_REGEX='(?!^k8s-(alpha)$)'" | tee -a $bashrc
  echo "export COOK_TEST_DEFAULT_SUBMIT_POOL=k8s-alpha" | tee -a $bashrc
  echo "export COOK_TEST_COMPUTE_CLUSTER_TYPE=kubernetes" | tee -a $bashrc
  echo "export COOK_TEST_DEFAULT_TIMEOUT_MS=480000" | tee -a $bashrc
  echo "export COOK_TEST_DEFAULT_WAIT_INTERVAL_MS=8000" | tee -a $bashrc
  cd $repo_root/integration || exit 1
  pip3 install -r requirements.txt

  # Cook Scheduler CLI setup
  cli=$repo_root/cli
  cd $cli || exit 1
  pip3 install -e .
  rm -f $HOME/.cs.json
  ln -s $cli/.cs.json $HOME/.cs.json
  SCRIPT
  config.vm.provision "shell", inline: $script, privileged: false
end
