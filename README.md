# Diagnose My Ceph Cluster

[![Build Status](https://travis-ci.org/hellboy2k8/DiagCephCluster.svg?branch=master)](https://travis-ci.org/hellboy2k8/DiagCephCluster)

Code base for for Diagnose my ceph cluster script


## Installation
 
See INSTALLATION.md


## Usage

To run do the following:

We can use username + password to authenticate:

```bash
diagnose_ceph -H host_ip -u username -p password
```

The other way is to use private key to authenticate:

```bash
diagnose_ceph -H host_ip -u username -k /home/penguinRaider/id_rsa
```
We can also use juju to run the script. Currently it requires vanilla installation of
juju(As the script parses and get the pem key and uses ubuntu user for few cases
rather than running it with juju run command)

```bash
diagnose_ceph --provider juju
```

**The following options are provided by the command line parser -**

## Options: ##
    -h, --help                                                          Display the Help Message 
    -H host_ip, --host host_ip                                          HOST_IP on which to connect
    -u username, --user username                                        username on the HOST_IP
    -p password, --pass password                                        password of the user(optional) 
    -P (juju, ssh)  --provider (juju, ssh)                              the provider to use(juju doesnt require most of the above parameters) 
    -k ssh_key_location --ssh_key ssh_key_location                      the address in the filesystem where the private key is located
    -t timeout --timeout timeout                                        the time for which to poll for status for the ceph health command(default=30)
    -a --advance                                                        option to toggle advance probe(like monmap replacement) (default=False)
## License
MIT Licensed
