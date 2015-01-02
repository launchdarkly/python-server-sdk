try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from pip.req import parse_requirements
import uuid


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=uuid.uuid1())

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='ldclient-py',
    version='0.13',
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
    ]
)