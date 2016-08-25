from setuptools import setup, find_packages

VERSION = '0.0.1'

setup(
    name='fed',
    version=VERSION,
    description="A federated search proxy and aggregator",
    long_description="""\
    A tornado-powered python library which queries a mix of local datasources as well as recursively querying remote Fed instances.""",
    author='Martin Holste',
    author_email='',
    url='http://github.com/mcholste/fed',
    download_url='http://github.com/mcholste/fed',
    license='Apache 2.0',
    packages=find_packages(exclude=['ez_setup', 'testes']),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache 2.0 License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Operating System :: OS Independent',
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'tornado>=4.4.0',
        'elasticsearch>=2.0.0',
    ],
    tests_require=[
        'unittest2',
        'nose'
    ],
    dependency_links=[],
)
