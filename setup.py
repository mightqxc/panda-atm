import sys

from setuptools import setup, find_packages
from pandaatm import panda_pkg_info

sys.path.insert(0, '.')

# get release version
release_version = panda_pkg_info.release_version

setup(
    name="pandaatm",
    version=release_version,
    description='ATM Package',
    long_description='''This package contains ATM components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://github.com/PanDAWMS/panda-atm/wiki',
    python_requires='>=3.6',
    packages=find_packages(),
    install_requires=[
                        'requests',
                        'python-daemon',
                        'cx_Oracle',
                        'panda-common',
                        'panda-server',
                        'numpy',
                        'sklearn',
                      ],
    data_files=[
        # # config and cron files
        # ('etc/panda', ['templates/panda_harvester.cfg.rpmnew.template',
        #                'templates/logrotate.d/panda_harvester',
        #                'templates/panda_harvester-httpd.conf.rpmnew.template',
        #                'templates/panda_supervisord.cfg.rpmnew.template',
        #                'templates/panda_harvester-uwsgi.ini.rpmnew.template',
        #                ]
        #  ),
        # # sysconfig
        # ('etc/sysconfig', ['templates/sysconfig/panda_harvester.rpmnew.template',
        #                    ]
        #  ),
        # # init script
        # ('etc/rc.d/init.d', ['templates/init.d/panda_harvester.rpmnew.template',
        #                      'templates/init.d/panda_harvester-apachectl.rpmnew.template',
        #                      'templates/init.d/panda_harvester-uwsgi.rpmnew.template',
        #                      ]
        #  ),
        # # admin tool
        # ('local/bin', ['templates/harvester-admin.rpmnew.template',
        #          ]
        #  ),
        ],
    scripts=[
                # 'templates/panda_jedi-renice',
                # 'templates/panda_harvester-sqlite3backup',
             ]
    )
