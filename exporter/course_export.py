#!/usr/bin/env python

"""
Export course data.

Usage:
  course-exporter [options]

Options:
  -h --help                  Show this screen.
  -n --dry-run               Don't run anything, just show what would be done.

  --work-dir=<dir>           Working directory.

  --limit=<limit>            Maximum number of results per file.

  --output-bucket=<bucket>   Destination bucket.
  --output-prefix=<pfx>      Prefix all output key names with this string.

  --external-prefix=<pfx>    Prefix relative paths to external files with this string.
  --pipeline-bucket=<pb>     Bucket that the EMR pipeline drops files in.
  --environment=<name>       The enviornment name for which this is being run, ex. prod, stage
  --se-bucket=<bucket>       The S3 bucket to retrieve StackExchange data from.


  --auth-file=<file>         Authentication file with credentials.

  --django-admin=<admin>            The path to the appropriate django-admin.py
  --django-pythonpath=<path>        The django python path
  --django-settings=<settings>      The path to lms django settings file
  --django-cms-settings=<settings>  The path to cms django settings file

  --lms-config=<path>       The path to lms config yml file
  --studio-config=<path>    The path to cms config yml file

  --mongo-host=<host>               The mongo host to connect
  --mongo-user=<username>           The mongo username to connect
  --mongo-password=<password>       The mongo password to connect
  --mongo-db=<db>                   The mongo db to query
  --mongo-collection=<collection>   The mongo collection to query

  --sql-host=<host>         The sql db host
  --sql-user=<user>         The sql username for login to db
  --sql-password=<password> The sql password for login to db
  --sql-db=<db>             The sql db to query

  --secret-key=<secert-key> The secret key to calculate user id hash

  --organization=<org-name> The name of the organization
"""


from contextlib import contextmanager
import datetime
import os
import logging
import logging.config
import re
import shutil

import boto3

from opaque_keys.edx.keys import CourseKey
from opaque_keys import InvalidKeyError

from exporter.tasks import CourseTask, FatalTaskError, OrgTask
from exporter.main import run_tasks, archive_directory, get_all_courses, _get_selected_tasks
from exporter.config import setup, get_config_for_course
from exporter.util import make_temp_directory, merge

log = logging.getLogger(__name__)


def main():
    general_config = setup(__doc__)

    courses = get_courses(general_config)

    with make_org_directory(general_config) as temp_directory:
        export_org_data(general_config, courses, temp_directory)

        for course in courses:
            config = get_config_for_course(general_config, course)
            course_directory = os.path.join(temp_directory, get_filename_safe_course_id(course))
            os.mkdir(course_directory)
            results = export_course_data(config, course_directory)
        
        root_dir = archive_directory(temp_directory)
        upload_files_or_dir(config, root_dir)


def export_org_data(config, courses, destination):
    kwargs = merge(config['values'], {})
    kwargs['courses'] = courses
    kwargs['work_dir'] = destination
    tasks_from_options = kwargs.get('tasks', [])

    org_tasks = _get_selected_tasks(OrgTask, tasks_from_options, [])
    run_tasks(org_tasks, **kwargs)


def get_courses(config):

    kwargs = merge(config['values'], {})
    all_courses  = get_all_courses(**kwargs)

    return all_courses


def archive_directory(directory):
    root_dir = os.path.dirname(directory)
    base_dir = os.path.basename(directory)

    # Fix for error when running make_archive from crontab
    os.chdir('/tmp')

    shutil.make_archive(directory, 'zip', root_dir, base_dir)
    shutil.rmtree(directory)

    return root_dir


def export_course_data(config, destination):
    log.info('Exporting data for %s', config['course'])

    results = []

    kwargs = merge(config, {})
    kwargs['work_dir'] = destination

    log.info("Getting data for course %s", config['course'])
    tasks_from_options = kwargs.get('tasks', [])
    course_tasks = _get_selected_tasks(CourseTask, tasks_from_options, [])

    filenames = run_tasks(course_tasks, **kwargs)
    results.extend(filenames)

    return results

def upload_files_or_dir(config, results_directory, sub_directory=None):

    if sub_directory:
        parent_directory = os.path.join(results_directory, sub_directory)
    else:
        parent_directory = results_directory

    for filename in os.listdir(parent_directory):
        filepath = os.path.join(parent_directory, filename)

        if(os.path.isdir(filepath)):
            upload_files_or_dir(config, results_directory, filename)
        else:
            if sub_directory:
                filename = os.path.join(sub_directory, filename)
            upload_file(config, filepath, filename)


def upload_file(config, filepath, filename):
    bucket = config['output_bucket']
    
    organization = config['organization']
    output_date = str(datetime.date.today())

    s3_target = '{prefix}_{org}/{date}/{name}'.format(
        prefix=organization,
        org=organization,
        date=output_date,
        name=filename
    )

    target = f's3://{bucket}/{s3_target}'

    log.info('Uploading file %s to %s', filepath, target)
    s3_client = boto3.client('s3')
    s3_client.upload_file(filepath, bucket, s3_target)


@contextmanager
def make_org_directory(config):
    org_dir = config['values']['work_dir']
    organization = config['values']['organization']

    prefix = '{0}_'.format(organization)

    with make_temp_directory(prefix=prefix, directory=org_dir) as temp_dir:
        # create working directory
        today = str(datetime.date.today())
        dir_name = '{name}-{date}'.format(name=organization, date=today)
        org_dir = os.path.join(temp_dir, dir_name)
        os.mkdir(org_dir)

        yield org_dir

def get_filename_safe_course_id(course_id, replacement_char='_'):
    """
    Create a representation of a course_id that can be used safely in a filepath.
    """
    try:
        course_key = CourseKey.from_string(course_id)
        filename = replacement_char.join([course_key.org, course_key.course, course_key.run])
    except InvalidKeyError:
        # If the course_id doesn't parse, we will still return a value here.
        filename = course_id

    # The safest characters are A-Z, a-z, 0-9, <underscore>, <period> and <hyphen>.
    # We represent the first four with \w.
    # TODO: Once we support courses with unicode characters, we will need to revisit this.
    return re.sub(r'[^\w\.\-]', replacement_char, filename)
