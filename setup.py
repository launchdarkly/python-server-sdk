import sys
try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import setup

from pip.req import parse_requirements
import uuid


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=uuid.uuid1())
test_reqs = parse_requirements('test-requirements.txt', session=uuid.uuid1())
twisted_reqs = parse_requirements('twisted-requirements.txt', session=uuid.uuid1())
redis_reqs = parse_requirements('redis-requirements.txt', session=uuid.uuid1())

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]
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
        import sys,subprocess
        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)

setup(
    name='ldclient-py',
    version='0.19.0',
    author='Catamorphic Co.',
    author_email='team@catamorphic.com',
    packages=['ldclient'],
    url='https://github.com/launchdarkly/python-client',
    description='LaunchDarkly SDK for Python',
    long_description='LaunchDarkly SDK for Python',
    install_requires=reqs,
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2 :: Only',
    ],
    extras_require={
        "twisted": txreqs,
        "redis": redisreqs
    },
    tests_require=testreqs,
    cmdclass = {'test': PyTest},
)