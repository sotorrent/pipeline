import setuptools

setuptools.setup(
    name='sotorrent-pipeline',
    url='https://github.com/sotorrent/pipeline',
    author='Sebastian Baltes',
    author_email='s@baltes.dev',
    version='0.1',
    install_requires=[
        'xmltodict>=0.12',
        'apache-beam[gcp]>=2.28',
        'google-cloud-bigquery==1.28.0',
        'google-cloud-storage>=1.37',
        'pyyaml>=5.3',
    ],
    packages=setuptools.find_packages('sotorrent'),
    package_data={
        '': ['config.yml'],
        'bigquery_schemas': ['*.json']
    }
)
