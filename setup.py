from setuptools import setup, find_packages

setup(name='graphio',
      version='0.0.6',
      description='Library to load data sets to Neo4j.',
      url='http://github.com/kaiser_preusse',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='Apache license 2.0',
      packages=find_packages(),
      install_requires=[
          'neo4j', 'py2neo'
      ],
      keywords=['NEO4J'],
      zip_safe=False,
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers'
      ],
      )
