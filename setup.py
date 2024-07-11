from setuptools import setup, find_packages
from pykrx import __version__

# main_ns = {}
# ver_path = convert_path('pykrx/version.py')
# with open(ver_path) as ver_file:
#     exec(ver_file.read(), main_ns)

with open("README.md", "r", encoding='UTF-8') as fh:
    long_description = fh.read()

setup(
    name='pykrx',
    version=__version__,
    description='KRX data scraping',
    url='https://github.com/liante0904/pykrx',
    author='Brayden Jo, Jonghun Yoo',
    author_email=('brayden.jo@outlook.com, '
                  'jonghun.yoo@outlook.com, '
                  'pystock@outlook.com'),
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=['requests', 'pandas', 'datetime', 'numpy', 'xlrd',
                      'deprecated', 'multipledispatch', 'matplotlib'],
    license='MIT',
    packages=find_packages(include=['pykrx', 'pykrx.*', 'pykrx.stock.*']),
    package_data={
        'pykrx': ['*.ttf'],
    },
    python_requires='>=3',
    zip_safe=False
)
