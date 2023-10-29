from setuptools import setup

setup(
    name='nrc_reactor',
    version='0.1.0',    
    url='https://github.com/nvrrember/Nuclear',
    description='A Python Package for Exploration of NRC Reactors dataset',
    author='Nate Cregut',
    author_email='natejc99@gmail.com',    
    license='BSD 2-clause',
    packages=['nrc_reactor'],
    install_requires=['pandas',
                      'requests',
                      ],

    classifiers=[
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',        
        'Programming Language :: Python :: 3',
    ],
)