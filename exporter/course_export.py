#!/usr/bin/env python

"""
Export course data.

Usage:
  course-exporter [options] <config> [--env=<environment>...] [--course=<course>...] [--task=<task>...]

Arguments:
  <config>                   YAML configuration file.
  --env=<environment>        Select environment. Can be specified multiple times.
  --task=<task>              Select task. Can be specified multiple times.
  --course=<course>             Select course. Can be specified multiple times.

Options:
  -h --help                  Show this screen.
  -n --dry-run               Don't run anything, just show what would be done.

  --work-dir=<dir>           Working directory.

  --limit=<limit>            Maximum number of results per file.

  --output-bucket=<bucket>   Destination bucket.
  --output-prefix=<pfx>      Prefix all output key names with this string.

  --external-prefix=<pfx>    Prefix relative paths to external files with this string.
  --pipeline-bucket=<pb>     Bucket that the EMR pipeline drops files in.
  --se-bucket=<bucket>       The S3 bucket to retrieve StackExchange data from.


  --auth-file=<file>         Authentication file with credentials.

  --django-admin=<admin>     The path to the appropriate django-admin.py
  --django-pythonpath=<path> The django python path
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

from exporter.tasks import CourseTask, FatalTaskError
from exporter.main import run_tasks, archive_directory, upload_data, get_all_courses, _get_selected_tasks
from exporter.config import setup, get_config_for_env, get_config_for_course
from exporter.util import make_temp_directory, with_temp_directory, merge

log = logging.getLogger(__name__)


def main():
    general_config = setup(__doc__)

    courses = get_courses(general_config)

    for course in courses:
        config = get_config_for_course(general_config, course)

        with make_course_directory(config, course) as temp_directory:
            results = export_course_data(config, temp_directory)
            root_dir = archive_directory(temp_directory)
            upload_files_or_dir(config, root_dir)

def get_courses(config):

    kwargs = config['values']
    all_courses  = get_all_courses(**kwargs)

    return all_courses


def archive_directory(directory):
    root_dir = os.path.dirname(directory)
    base_dir = os.path.basename(directory)

    shutil.make_archive(directory, 'zip', root_dir, base_dir)
    shutil.rmtree(directory)

    return root_dir


def export_course_data(config, destination):
    log.info('Exporting data for %s', config['course'])

    results = []

    kwargs = config
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
    prefix = config['output_prefix'] or ''
    filename_safe_course_id = get_filename_safe_course_id(config['course'])
    output_date = str(datetime.date.today())

    target = 's3://{bucket}/{prefix}{course}/state/{date}/{name}'.format(
            bucket=bucket,
            prefix=prefix,
            course=filename_safe_course_id,
            date=output_date,
            name=filename
        )

    log.info('Uploading file %s to %s', filepath, target)
    s3_client = boto3.client('s3')
    s3_client.upload_file(filepath, bucket, '{prefix}_{course}/{date}/{name}'.format(
        prefix=prefix,
        course=filename_safe_course_id,
        date=output_date,
        name=filename
    ))


@contextmanager
def make_course_directory(config, course):
    filename_safe_course = get_filename_safe_course_id(course)
    course_dir = config['work_dir']

    prefix = '{0}_'.format(filename_safe_course)

    with make_temp_directory(prefix=prefix, directory=course_dir) as temp_dir:
        # create working directory
        today = str(datetime.date.today())
        dir_name = '{name}-{date}'.format(name=filename_safe_course, date=today)
        course_dir = os.path.join(temp_dir, dir_name)
        os.mkdir(course_dir)

        yield course_dir

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
