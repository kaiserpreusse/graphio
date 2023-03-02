from setuptools import setup, find_packages

from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='graphio',
      use_scm_version={
          "root": ".",
          "relative_to": __file__,
          "local_scheme": "node-and-timestamp"
      },
      setup_requires=['setuptools_scm'],
      description='Library to load data sets to Neo4j.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/kaiserpreusse/graphio',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='Apache License 2.0',
      packages=find_packages(),
      install_requires=[
          'py2neo>=2021.0.0', 'neo4j>=5.2'
      ],
      keywords=['NEO4J'],
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License'
      ],
      )
