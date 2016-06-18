from distutils.core import setup
import sys

if sys.version_info < (3,5):
    sys.exit("This package required >=python3.5, please upgrade")

setup(
    name='exchange',
    version='1.0',
    author='Jan Škoda',
    author_email='skoda@jskoda.cz',
    packages=['exchange'],
    scripts=['exchange-simulator.py'],
    url='http:/github.com/lefty/exchange',
    license='GNU/GPLv3',
    description='TCP server for simulating simple stock exchange',
    long_description=open('readme.md').read(),
)