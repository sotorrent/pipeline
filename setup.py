import setuptools

setuptools.setup(
    name='sotorrent_pipeline',
    url='https://github.com/sotorrent/pipeline',
    author='Sebastian Baltes',
    author_email='s@baltes.dev',
    version='0.1.1',
    license='Apache-2.0',
    install_requires=[
        'apache-beam[gcp]>=2.28',
        'google-cloud-bigquery==1.28.0',
        'google-cloud-storage>=1.37'
    ],
    packages=setuptools.find_packages(include=['sotorrent_pipeline', 'sotorrent_pipeline.*']),
    package_data={
        'sotorrent_pipeline': ['bigquery_schemas/*.json', 'type_tables/*.jsonl']
    },
    entry_points={
        'console_scripts': ['sotorrent-pipeline=sotorrent_pipeline.main:main']
    }
)
