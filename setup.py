from pathlib import Path

from setuptools import find_packages, setup

here = Path(__file__).absolute().parent

long_description = (here / Path('README.md')).read_text()

_version = {}
exec((here / Path('datums_warehouse/_version.py')).read_text(), _version)

setup(
    name='datums_warehouse',
    version=_version['__version__'],
    description='Data warehouse that provides time series data via REST API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/SwamyDev/datums-warehouse',
    author='Bernhard Raml',
    packages=find_packages(include=['datums_warehouse', 'datums_warehouse.*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=['flask', 'werkzeug', 'pandas', 'numpy', 'requests', 'click', 'uwsgi', 'wheel', 'more_itertools'],
    extras_require={"test": ["pytest", "pytest-cov"]},
    scripts=['scripts/update_warehouse'],
    python_requires='>=3.6'
)
