from setuptools import setup

setup(name='graphio',
      version='0.0.2',
      description='Library to load data sets to Neo4j.',
      url='http://github.com/kaiser_preusse',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='Apache license 2.0',
      packages=['graphio'],
      install_requires=[
          'test_neo4j', 'py2neo'
      ],
      keywords=['NEO4J'],
      zip_safe=False,
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers'
      ],
      )
