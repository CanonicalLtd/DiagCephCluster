# Diagnose My Ceph Cluster


Code base for for Diagnose my ceph cluster script

## Usage

To run do the following:


```bash
python run.py -H host_ip -u username -p password
```


**The following options are provided by the command line parser -**

## Options: ##
    -h, --help                                                          Display the Help Message 
    -H host_ip, --host host_ip                                          HOST_IP on which to connect
    -u username, --user username                                        username on the HOST_IP
    -p password, --pass password                                        password of the user(optional) 
    -P (juju, ssh)  --provider (juju, ssh)                              the provider to use(juju doesnt require most of the above parameters, currently only supports ssh) 

## License
MIT Licensed
