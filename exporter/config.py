# pylint: disable=missing-docstring

import json
import logging
import logging.config
import os
import tempfile

from docopt import docopt
import yaml

from exporter.util import merge, filter_keys

log = logging.getLogger(__name__)


def setup(doc, argv=None):
    program_options = docopt(doc, argv=argv)
    setup_logging()

    log.info('Reading configuration')

    return _get_config(program_options)


def _get_config(program_options):
    config = {}
    
    update_config(config, program_options)

    set_config_defaults(config)

    return config


def set_config_defaults(config):
    values = config['values']

    if not values.get('lms_config'):
        values['lms_config'] = '/edx/etc/lms.yml'

    if not values.get('studio_config'):
        values['studio_config'] = '/edx/etc/studio.yml'

    if not values.get('django_admin'):
        values['django_admin'] = 'django-admin'

    if not values.get('django_pythonpath'):
        values['django_pythonpath'] = '/edx/app/edxapp/edx-platform'


def update_config(config, program_options):
    merge_program_options(config, program_options)
    # Config files may not always contain organization information.
    if 'organizations' in config:
        update_organizations(config)
    update_tasks(config)


def merge_program_options(config, program_options):
    # get program options, removing '--' and replacing '-' with '_'
    options = {k[2:].replace('-', '_'): v for k, v
               in program_options.items()
               if k.startswith('--')}

    if not options.get('work_dir'):
        options['work_dir'] = tempfile.gettempdir()

    config['values'] = options


def update_organizations(config):
    values = config['values']

    # lowercase orgs before selection
    organizations = {org.lower(): values for org, values
                     in config['organizations'].items()}

    # select only organizations in arguments
    organizations = filter_keys(organizations, values.get('org'))

    config['organizations'] = organizations


def update_tasks(config):
    values = config['values']
    tasks = values.get('task', []) or values.get('tasks', [])

    if 'task' in values:
        del values['task']

    if tasks:
        values['tasks'] = tasks


def get_config_for_org(config, organization):
    org_config = merge(config['organizations'][organization], config['values'])
    org_config['organization'] = organization
    org_config['environments'] = config['environments']
    return org_config


def get_config_for_course(config, course):
    # config['values'] are overridden default values with program options, every other key is from the config file.
    course_config = merge(config['values'], {})
    course_config['course'] = course
    return course_config


def get_config_for_env(config, environment):
    env_config = merge(config, config['environments'][environment])
    env_config['environment'] = environment
    return env_config


def setup_logging():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            }
        },
        'handlers': {
            'default': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'INFO',
                'propagate': True
            }
        }
    })
