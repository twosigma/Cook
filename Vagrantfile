Vagrant.configure("2") do |config|
  config.vm.box = "hashicorp/bionic64"
  config.vm.provision :shell, path: "scheduler/bin/bootstrap", env: {"GKE_CLUSTER_OWNER" => ENV["USER"]}
end
