# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
    # Provision using shell
    config.vm.provision "shell", path: "vagrant/setup.sh"

    config.vm.define "superelasticsearch", primary: true do |superelasticsearch|
        # Box configuration
        superelasticsearch.vm.box = "debian-7.3.0"
        superelasticsearch.vm.box_url = "http://puppet-vagrant-boxes.puppetlabs.com/debian-73-x64-virtualbox-nocm.box"

        # Synced folders
        superelasticsearch.vm.synced_folder ".", "/home/vagrant/superelasticsearch"
    end
end
