from setuptools import setup, find_packages
from distutils.util import convert_path

main_ns = {}
ver_path = convert_path('mpykrx/version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

with open("README.md", "r", encoding='UTF-8') as fh:
    long_description = fh.read()

setup(
    name            = 'mpykrx',
    version         = main_ns['__version__'],
    description     = 'KRX data scraping',
    url             = 'https://github.com/HyuntaMansei/pykrx',
    author          = 'Brayden Jo, Jonghun Yoo',
    author_email    = 'brayden.jo@outlook.com, jonghun.yoo@outlook.com, pystock@outlook.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires= ['requests', 'pandas', 'datetime', 'numpy', 'xlrd', 'deprecated'],
    license         = 'MIT',
    packages        = find_packages(include=['mpykrx', 'mpykrx.*', 'mpykrx.stock.*']),
    python_requires = '>=3',
    zip_safe        = False
)