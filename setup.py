from pip.req import parse_requirements
from setuptools import setup, find_packages
import sys

desc = 'CLI script to diagnose a broken/partially working ceph cluster'
test_module = 'nose.collector'
install_reqs = parse_requirements("./requirements.txt", session=False)
reqs = [str(ir.req) for ir in install_reqs]

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
    install_requires=reqs,
    test_suite=test_module,
)
