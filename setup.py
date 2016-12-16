from setuptools import setup, find_packages

version = '0.1.2'

setup(name='poliglo',
      version=version,
      description="Python client for poliglo (https://github.com/dperezrada/poliglo)",
      long_description="""\
""",
      classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Daniel P\xc3\xa9rez Rada',
      author_email='dperezrada@gmail.com',
      url='https://github.com/dperezrada/poliglo-py',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'redis'
      ],
      tests_require=['nose'],
      test_suite="tests",
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
