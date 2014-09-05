try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='ldclient-py',
    version='0.2',
    author='Catamorphic Co.',
    author_email='team@catamorphic.com',
    packages=['ldclient'],
    url='https://github.com/launchdarkly/python-client',
    description='LaunchDarkly SDK for Python',
    long_description='LaunchDarkly SDK for Python',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2 :: Only',
    ]
)