from setuptools import setup, find_packages
import sys

desc = 'CLI script to diagnose a broken/partially working ceph cluster'

setup(
    name='diagnose_ceph',
    version='0.1.0alpha',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'diagnose_ceph': ['diagnose_ceph/scripts/*.sh'],
    },
    entry_points={
        'console_scripts': ['diagnose_ceph=diagnose_ceph.run:run', ],
    },
    license='MIT',
    description=desc,
    long_description=open("README.md").read(),
    install_requires=['paramiko', 'pycodestyle', 'unittest2',
                      'nose', ],
    test_suite='nosetests -s',
)
