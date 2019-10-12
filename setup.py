from setuptools import find_packages, setup

setup(
    name='datums_warehouse',
    version='0.0.0',
    packages=find_packages(include='datums_warehouse'),
    include_package_data=True,
    zip_safe=False,
    install_requires=['flask'],
    extras_require={"test": ["pytest", "coverage"]},
)
