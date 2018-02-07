try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import setup

import sys
import uuid

from pip.req import parse_requirements

ldclient_version='4.3.0'

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=uuid.uuid1())
python26_reqs = parse_requirements('python2.6-requirements.txt', session=uuid.uuid1())
test_reqs = parse_requirements('test-requirements.txt', session=uuid.uuid1())
twisted_reqs = parse_requirements(
    'twisted-requirements.txt', session=uuid.uuid1())
redis_reqs = parse_requirements('redis-requirements.txt', session=uuid.uuid1())

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]
python26reqs = [str(ir.req) for ir in python26_reqs]
testreqs = [str(ir.req) for ir in test_reqs]
txreqs = [str(ir.req) for ir in twisted_reqs]
redisreqs = [str(ir.req) for ir in redis_reqs]


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys
        import subprocess
        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)

setup(
    name='ldclient-py',
    version=ldclient_version,
    author='LaunchDarkly',
    author_email='team@launchdarkly.com',
    packages=['ldclient'],
    url='https://github.com/launchdarkly/python-client',
    description='LaunchDarkly SDK for Python',
    long_description='LaunchDarkly SDK for Python',
    install_requires=reqs,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
    ],
    extras_require={
        "twisted": txreqs,
        "redis": redisreqs,
        "python2.6": python26reqs
    },
    tests_require=testreqs,
    cmdclass={'test': PyTest},
)
