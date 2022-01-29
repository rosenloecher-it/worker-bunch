from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

# this grabs the requirements from requirements.txt
install_requires = [i.strip() for i in open("requirements.txt").readlines()]

VERSION = '0.1.0'
DESCRIPTION = 'Streaming video data via networks'

# Setting up
setup(
    name="worker-bunch",
    version=VERSION,
    author="Rosenloecher-IT (Raul Rosenlöcher)",
    author_email="<github@rosenloecher-it.de>",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=install_requires,
    keywords=['smarthome', 'mqtt', 'rule-engine', 'task-engine'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
    ]
)
