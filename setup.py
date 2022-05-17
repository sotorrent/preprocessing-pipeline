import setuptools

setuptools.setup(
    name='preprocessing_pipeline',
    url='https://github.com/sotorrent/preprocessing-pipeline',
    author='Sebastian Baltes',
    author_email='s@baltes.dev',
    version='0.0.1',
    license='Apache-2.0',
    install_requires=[
        'apache-beam[gcp]~=2.38.0'
    ],
    packages=setuptools.find_packages(include=['preprocessing_pipeline', 'preprocessing_pipeline.*']),
    package_data={},
    entry_points={
        'console_scripts': ['preprocessing-pipeline=preprocessing_pipeline.main:main']
    }
)
