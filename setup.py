from distutils.core import setup
import platform

scripts = ['cardiff=cardiff.controller:main']

requirements = ['clihelper',
                'flatdict',
                'pyyaml',
                'tornado']

# Build the path to install the templates, example config and static files
base_path = '/usr/local/share/cardiff'

data_files = {'%s/' % base_path: ['README.md', 'etc/example.yaml'],
              '%s/init.d/' % base_path: ['etc/init.d/cardiff']}

with open('MANIFEST.in', 'w') as handle:
    for path in data_files:
        for filename in data_files[path]:
            handle.write('include %s\n' % filename)

tests_require = ['mock']
(major, minor, rev) = platform.python_version_tuple()
if float('%s.%s' % (major, minor)) < 2.7:
    tests_require.append('unittest2')

setup(name='cardiff',
      version='1.0.0',
      description='Python statsd clone',
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='http://github.com/gmr/cardiff',
      packages=['cardiff', 'cardiff.backends'],
      license='BSD',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: BSD License',
          'Natural Language :: English',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Topic :: System :: Monitoring',
          'Topic :: Utilities'
          ],
      install_requires=requirements,
      extras_require={'amqp': ['rmqid']},
      tests_require=tests_require,
      data_files=[(key, data_files[key]) for key in data_files.keys()],
      entry_points=dict(console_scripts=scripts),
      zip_safe=True)
