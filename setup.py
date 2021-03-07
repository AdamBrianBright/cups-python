import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='cups-python',
    author='Adam Bright',
    author_email='adam.brian.bright@gmail.com',
    description='Python 3.8 Cifrazia Ultimate Permission System',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/AdamBrianBright/cups-python',
    packages=setuptools.find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    install_requires=[
        "py2neo >= 2021.0.1",
        "pytz >= 2021.1",
        "ujson >= 4.0.2",
    ],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    classifiers=[
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
