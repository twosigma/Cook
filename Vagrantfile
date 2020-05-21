Vagrant.configure("2") do |config|
  config.vm.box = "hashicorp/bionic64"

  config.vm.provider "virtualbox" do |v|
    v.memory = 6144
    v.cpus = 2
  end

  # This runs as root:
  config.vm.provision :shell, path: "scheduler/bin/bootstrap", env: {"GKE_CLUSTER_OWNER" => ENV["USER"], "GCP_PROJECT_NAME" => ENV["GCP_PROJECT_NAME"]}

  # This runs as vagrant:
  $script = <<-SCRIPT
  # Cook jobclient setup
  cd /vagrant/jobclient || exit 1
  mvn install -DskipTests
  SCRIPT
  config.vm.provision "shell", inline: $script, privileged: false
end
