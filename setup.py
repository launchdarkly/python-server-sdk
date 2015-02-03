import sys
try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand
except ImportError:
    from distutils.core import setup

from pip.req import parse_requirements
import uuid


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=uuid.uuid1())

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name='ldclient-py',
    version='0.14.0',
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
    tests_require=['pytest'],
    cmdclass = {'test': PyTest},
)