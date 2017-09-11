from setuptools import setup
from azkaban_orchestrator import __version__

REQUIREMENTS = [
    'requests == 2.13.0',
    'graphviz == 0.8'
]

setup(
    name='azkaban-orchestrator',
    version=__version__,
    description='Azkaban Orchestraor',
    author='Telegraph Data Platform team',
    packages=['azkaban_orchestrator'],
    test_suite="nose.collector",
    tests_require=['mock==2.0.0', 'nose'],
    install_requires=REQUIREMENTS
)
