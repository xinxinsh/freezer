"""
Copyright 2015 Hewlett-Packard

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
from oslo_config import cfg
from oslo_log import log
import sys

from freezer import __version__ as FREEZER_VERSION

CONF = cfg.CONF
_LOG = log.getLogger(__name__)



from freezer.apiclient import client as api_client
from freezer import winutils

if winutils.is_windows():
    DEFAULT_FREEZER_SCHEDULER_CONF_D = r'C:\.freezer\scheduler\conf.d'
else:
    DEFAULT_FREEZER_SCHEDULER_CONF_D = '/etc/freezer/scheduler/conf.d'


def getCommonOpts():
    scheduler_conf_d = os.environ.get('FREEZER_SCHEDULER_CONF_D',
                                      DEFAULT_FREEZER_SCHEDULER_CONF_D)

    common_opts = [
    cfg.StrOpt('job',
               default=None,
               dest='job_id',
               short='j',
               help='Name or ID of the job'),
    cfg.StrOpt('session',
               default=None,
               dest='session_id',
               short='s',
               help='Name or ID of the session'),
    cfg.StrOpt('file',
               default=None,
               dest='fname',
               help='Local file that contains the resource to be '
                    'uploaded/downloaded'),
    cfg.StrOpt('client-id',
               default=None,
               dest='client_id',
               short='c',
               help='Specifies the client_id used when contacting the service.'
                    '\n If not specified it will be automatically created \n'
                    'using the tenant-id and the machine hostname.'),
    cfg.BoolOpt('no-api',
               default=False,
               dest='no_api',
               short='n',
               help='Prevents the scheduler from using the api service'),
    cfg.BoolOpt('active-only',
               default=False,
               dest='active_only',
               short='a',
               help='Filter only active jobs/session'),
    cfg.StrOpt('conf',
               default=scheduler_conf_d,
               dest='jobs_dir',
               short='f',
               help='Used to store/retrieve files on local storage, including '
                    'those exchanged with the api service.Default value is {0} '
                    '(Env: FREEZER_SCHEDULER_CONF_D)'.format(scheduler_conf_d)),
    cfg.IntOpt('interval',
               default=60,
               dest='interval',
               short='i',
               help='Specifies the api-polling interval in seconds. '
                    'Defaults to 60 seconds'),
    cfg.BoolOpt('no-daemon',
               default=False,
               dest='no_daemon',
               help='Prevents the scheduler from running in daemon mode'),
    cfg.BoolOpt('insecure',
               default=False,
               dest='insecure',
               help='Initialize freezer scheduler with insecure mode'),
    ]

    return common_opts


def parse_args(choices):
    default_conf = cfg.find_config_files('freezer', 'scheduler',
                                         '.conf')
    CONF.register_cli_opts(api_client.build_os_options())
    CONF.register_cli_opts(getCommonOpts())
    log.register_options(CONF)

    positional = [
        cfg.StrOpt('action',
                   choices=choices,
                   default=None,
                   help='{0}'.format(choices), positional=True),

    ]
    CONF.register_cli_opts(positional)
    CONF(args=sys.argv[1:],
         project='freezer-scheduler',
         default_config_files=default_conf,
         version=FREEZER_VERSION
         )


def setup_logging():
    _DEFAULT_LOG_LEVELS = ['amqp=WARN', 'amqplib=WARN', 'boto=WARN',
                       'qpid=WARN', 'stevedore=WARN',
                       'oslo_log=INFO', 'iso8601=WARN',
                       'requests.packages.urllib3.connectionpool=WARN',
                       'urllib3.connectionpool=WARN', 'websocket=WARN',
                       'keystonemiddleware=WARN', 'freezer=INFO']

    _DEFAULT_LOGGING_CONTEXT_FORMAT = ('%(asctime)s.%(msecs)03d %(process)d '
                                       '%(levelname)s %(name)s [%(request_id)s '
                                       '%(user_identity)s] %(instance)s'
                                       '%(message)s')
    log.set_defaults(_DEFAULT_LOGGING_CONTEXT_FORMAT, _DEFAULT_LOG_LEVELS)
    log.setup(CONF, 'freezer-scheduler', version=FREEZER_VERSION)


def list_opts():
    _opt = {
        None: getCommonOpts()
    }
    return _opt.items()
