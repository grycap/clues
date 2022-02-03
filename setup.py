#!/usr/bin/env python
from version import VERSION
from setuptools import setup
from setuptools.command.install import install
import distutils.archive_util
import os
import sys

# apt-get install python-mysqldb
# tar xfz cluesonebindings-0.28.tar.gz
# cd cluesonebindings-0.28
# python setup.py install
# cd ..
# tar xfz CLUES-2.0.1.tar.gz
# cd CLUES-2.0.1
# python setup.py install
# /etc/init.d/cluesd start
# crear configuracion /etc/clues2/clues2.cfg y /etc/clues2/conf.d/plugin-one.cfg

#mkdir -p /var/log/clues/ /var/lib/clues2/ /etc/clues2/
#cp etc/conf.d/plugin-example.cfg /etc/clues2/
#cp etc/clues2.cfg /etc/clues2/
#wget https://github.com/grycap/cpyutils/archive/master.zip
#unzip master.zip 
#cd cpyutils-master
#python setup.py install --record installed-files.txt
#cd ..
#easy_install ply
#easy_install web.py

# oneuser create clues cluespass
# oneuser chgrp clues oneadmin
# cp conf.d/plugin-one.cfg-example conf.d/plugin-ipmi.cfg-example /etc/clues2/conf.d/

# Avoid using wheel as it does not copy data_files to / dir
if 'bdist_wheel' in sys.argv:
    raise RuntimeError("This setup.py does not support wheels")

class my_install(install):
    def touch(self, fname):
        if os.path.exists(fname):
            os.utime(fname, None)
        else:
            open(fname, 'a').close()
            
    def run(self):
        install.run(self)

        # We create the /var/log/clues2 directory to be the default log directory
        distutils.archive_util.mkpath("/var/log/clues2", mode=0o0777)
        self.touch("/var/log/clues2/clues2.log")
        self.touch("/var/log/clues2/clues2-cli.log")

        # We set the file /var/log/clues2/clues2.log file to 0o666 to be able to be written when the users submit the jobs using the pbs filter
        os.chmod("/var/log/clues2/clues2.log", 0o666)
        os.chmod("/var/log/clues2/clues2-cli.log", 0o666)

        # We create the /var/lib/clues2 directory to be the default for pid file and db
        distutils.archive_util.mkpath("/var/lib/clues2", mode=0o0750)

        # We set the permissions of the configuration folder to be only readable by the one that installs CLUES (to avoid users to use commandline)
        os.chmod("/etc/clues2", 0o750)
        distutils.archive_util.mkpath("/etc/clues2/scripts", mode=0o0777)


setup(name='CLUES',
      version=VERSION,
      description='CLUES - CLUster Energy-saving System - version 2', 
      author='Carlos de Alfonso',
      author_email='caralla@upv.es',
      url='http://www.grycap.upv.es/clues',
      # package_dir = {'cluesonebindings':'../cluesonebindings'},
      packages = [ 'clueslib', 'cluesplugins', 'clues' ],
      package_dir = { 'clues': '.' },
      # py_modules = [ 'configcli', 'configserver' ],
      data_files = [
        ('/etc/clues2/', [
            'etc/clues2.cfg-example',
            'etc/clues2.cfg-full-example',
            'etc/clues2.cfg-cli-example',
            'etc/virtualonecluster.hosts-example',
            'etc/ipmi.hosts-example'
            ] ),
        ('/etc/clues2/conf.d/', [
            'etc/conf.d/plugin-one.cfg-example',
            'etc/conf.d/wrapper-one.cfg-example',
            'etc/conf.d/plugin-im.cfg-example',
            'etc/conf.d/plugin-pbs.cfg-example',
            'etc/conf.d/plugin-sge.cfg-example',
            'etc/conf.d/wrapper-sge.cfg-example',
            'etc/conf.d/plugin-slurm.cfg-example',
            'etc/conf.d/plugin-commandline.cfg-example',
            'etc/conf.d/hooks.cfg-example',
            'etc/conf.d/plugin-wol.cfg-example',
            'etc/conf.d/plugin-kubernetes.cfg-example',
            'etc/conf.d/plugin-nomad.cfg-example',
            'etc/conf.d/plugin-ipmi.cfg-example',
            'etc/conf.d/plugin-mesos.cfg-example',
            'etc/conf.d/schedulers.cfg-example']),
        ('/etc/clues2/reports/', [
            'etc/reports/cluescharts.js',
            'etc/reports/cluesdata.js',
            'etc/reports/cluesstats.js',
            'etc/reports/minibarchart.js',
            'etc/reports/cluesprocess.js',
            'etc/reports/piechart.js',
            'etc/reports/randomColor.js',
            'etc/reports/index.html']),
        ('/etc/clues2/simulator', [
            'etc/simulator/simple.sim',
            'etc/simulator/platform.sim'
        ]),
        ('/etc/logrotate.d/', [
            'etc/clues-logrotate'
            ]),
        ('/etc/init.d', [
            'cluesd'
            ]),
        # force cluesserver to be in /usr/bin
        ('/usr/bin', [
            'cluesserver'
            ]),
        ('/etc/systemd/system', [
            'cluesd.service'
            ])
        ],
      scripts = [ 'clues', 'cluesserver', 'cluesreports', 'addons/pbs/clues-pbs-wrapper', 'addons/one/clues-one-wrapper', 'addons/sge/clues-sge-wrapper', 'addons/slurm/clues-slurm-wrapper' ],
      requires = [ 'cpyutils (>= 0.40)' ],
      cmdclass={'install': my_install}
)
