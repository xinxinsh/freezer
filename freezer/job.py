"""
(c) Copyright 2014,2015 Hewlett-Packard Development Company, L.P.
(C) Copyright 2016 Hewlett Packard Enterprise Development Company LP

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

import abc
import datetime
import sys
import time

from oslo_config import cfg
from oslo_log import log
from oslo_utils import importutils
import six

from freezer.exceptions import utils as utils_exception
from freezer.openstack import backup as backup_service
from freezer.openstack import restore as restore_service
from freezer.openstack import admin as admin_service
from freezer.utils import exec_cmd
from freezer.utils import utils
from freezer.utils import backup as db
from freezer.utils import quota

CONF = cfg.CONF
LOG = log.getLogger(__name__)
QUOTA = quota.QUOTA


@six.add_metaclass(abc.ABCMeta)
class Job(object):
    """
    :type storage: freezer.storage.base.Storage
    :type engine: freezer.engine.engine.BackupEngine
    """
    def __init__(self, conf_dict, storage):
        self.conf = conf_dict
        self.storage = storage
        self.engine = conf_dict.engine
        self._general_validation()
        self._validate()

    @abc.abstractmethod
    def _validate(self):
        """
        Method that validates if all arguments available to execute the job
        or not
        :return: True or raise an error
        """
        pass

    def _general_validation(self):
        """
        Apply general validation rules.
        :return: True or raise an error
        """
        LOG.info("Validating args for the {0} job.".format(self.conf.action))
        if not self.conf.action:
            raise ValueError("Please provide a valid action with --action")

    @abc.abstractmethod
    def execute(self):
        pass


class InfoJob(Job):

    def _validate(self):
        # no validation required for this job
        pass

    def execute(self):
        info = self.storage.info()

        if not info:
            return
        fields = ["Backup ID", "Source ID", "Status", "Name", "Size", "Object Count", "Container"]
        data = []
        for container in info:
            values = [
                container.get('backup_id'),
                container.get('source_id'),
                container.get('status'),
                container.get('name'),
                container.get('size'),
                container.get('objects_count'),
                container.get('container')
            ]
            data.append(values)
        return [fields, data]


class BackupJob(Job):

    def _validate(self):
        if self.conf.backup_media == 'nova' \
                and not self.conf.nova_inst_id:
            raise ValueError(" --nova_inst_id should be set")
        elif self.conf.backup_media == 'cindernative' \
                and not self.conf.cindernative_vol_id:
            raise ValueError("--cindernative_vol_id should be set")
        elif self.conf.backup_media == 'trove' \
                and not self.conf.trove_instance_id:
            raise ValueError("--trove_instance_id should be set")

        if not self.conf.container:
            raise ValueError("--container is required")

    def execute(self):
        LOG.info('Backup job started. '
                 'backup_name: {0}, container: {1}, hostname: {2}, mode: {3},'
                 ' Storage: {4}, compression: {5}'
                 .format(self.conf.backup_name, self.conf.container,
                         self.conf.hostname, self.conf.mode, self.conf.storage,
                         self.conf.compression))
        if not self.conf.mode:
            raise ValueError("Empty mode")

        mod_name = 'freezer.mode.{0}.{1}'.format(
            self.conf.mode, self.conf.mode.capitalize() + 'Mode')
        app_mode = importutils.import_object(mod_name, self.conf)

        backup_os = backup_service.BackupOs(self.conf.client_manager,
                                            self.conf.container,
                                            self.storage)
        try:
            size = 1
            backup_media = self.conf.backup_media
            if backup_media == 'nova':
                size = backup_os.get_nova_size(self.conf.nova_inst_id)
            elif backup_media == 'cindernative':
                size = backup_os.get_cinder_size(self.conf.cindernative_vol_id)
            elif backup_media == 'trove':
                """TODO:pls, implement get_trove_size method"""
                size = backup_os.get_trove_size(self.conf.trove_instance_id)

            reserve_opts = {'backups': 1,
                            'backup_bytes': size}
            QUOTA.reserve(**reserve_opts)
        except Exception as e:
            LOG.error('Executing {0} backup failed'.format(
                self.conf.backup_media))
            LOG.exception(e)
            raise utils_exception.ExceedQuotaException(e.message)

        self.conf.time_stamp = utils.DateTime.now().timestamp
        backup = None
        try:
            kwargs = {
                'curr_backup_level': 0,
                'client_os': sys.platform,
                'description': self.conf.description,
                'client_version': self.conf.__version__,
                'time_stamp': self.conf.time_stamp,
                'end_time_stamp': self.conf.time_stamp,
                'action': self.conf.action,
                'always_level': self.conf.always_level,
                'backup_name': self.conf.backup_name,
                'hostname_backup_name': self.conf.hostname_backup_name,
                'container': self.conf.container,
                'hostname': self.conf.hostname,
                'storage': self.conf.storage,
                'mode': self.conf.mode,
                'size': 0,
                'status': db.BackupStatus.CREATING,
                'compression': self.conf.compression,
                'consistency_checksum': self.conf.consistency_checksum,
            }
            backup = db.Backup(**kwargs)
            backup.create()
            self.backup(app_mode, backup_os, backup)
        except Exception as e:
            LOG.error('Executing {0} backup failed'.format(
                self.conf.backup_media))
            LOG.exception(e)
            QUOTA.rollback(**reserve_opts)
            if backup and 'backup_id' in backup:
                backup.status = db.BackupStatus.ERROR
                backup.failed_reason = e.message
                backup.end_time_stamp = utils.DateTime.now().timestamp
                backup.save()

        backup.status = db.BackupStatus.AVAILABLE
        backup.end_time_stamp = utils.DateTime.now().timestamp
        backup.save()

        return backup.to_primitive()

    def backup(self, app_mode, backup_os, db_backup):
        """
        :param app_mode:
        :param backup_os: freezer.openstack.backup
        :param db_backup: backup dict
        :return:
        """
        backup_media = self.conf.backup_media
        backup_meta = None
        if backup_media == 'nova':
            LOG.info('Executing nova backup. Instance ID: {0}'.format(
                self.conf.nova_inst_id))
            backup_meta = backup_os.backup_nova(self.conf.nova_inst_id,
                                                name=self.conf.backup_name,
                                                incremental=self.conf.incremental,
                                                backup=db_backup)
            db_backup.backup_chain_name = backup_meta.backup_chain_name
        elif backup_media == 'cindernative':
            LOG.info('Executing cinder native backup. Volume ID: {0}, '
                     'incremental: {1}'.format(self.conf.cindernative_vol_id,
                                               self.conf.incremental))
            backup_meta = backup_os.backup_cinder(self.conf.cindernative_vol_id,
                                                  name=self.conf.backup_name,
                                                  incremental=self.conf.incremental,
                                                  backup=db_backup)
            db_backup.backup_chain_name = backup_meta['backup_chain_name']
            db_backup.backend_id = backup_meta['id']
            db_backup.size = backup_meta['size']
        elif backup_media == 'trove':
            LOG.info('Executing trove backup. Instance ID: {0}, '
                     'incremental: {1}'.format(self.conf.trove_instance_id,
                                               self.conf.incremental))
            backup_os.backup_trove(self.conf.trove_instance_id,
                                   name=self.conf.backup_name,
                                   incremental=self.conf.incremental,
                                   backup=db_backup)
        else:
            raise Exception('unknown parameter backup_media %s' % backup_media)
        return backup_meta


class RestoreJob(Job):

    def _validate(self):
        if self.conf.backup_media == 'nova' \
                and not self.conf.nova_inst_id \
                and not self.nova_backup_id:
            raise ValueError("either --nova_inst_id or --nova_backp_id should be set")
        elif self.conf.backup_media == 'cindernative' \
                and not self.conf.cindernative_vol_id \
                and not self.conf.cindernative_backup_id:
            raise ValueError("either --cindernative_vol_id or --cindernative_backup_id should be set")
        elif self.conf.backup_media == 'trove' \
                and not self.conf.trove_instance_id \
                and not self.conf.trove_backup_id:
            raise ValueError("either --trove_instance_id or --trove_backup_id should be set")

        if not self.conf.container:
            raise ValueError("--container is required")
        if self.conf.no_incremental and (self.conf.max_level or
                                         self.conf.always_level):
            raise Exception(
                'no-incremental option is not compatible '
                'with backup level options')

    def execute(self):
        LOG.info('Executing Restore...')
        backup_media = self.conf.backup_media

        backup = None
        source_id = None
        if backup_media == 'nova':
            source_id = self.conf.nova_inst_id
        elif backup_media == 'cindernative':
            source_id = self.conf.cindernative_vol_id
        elif backup_media == 'trove':
            source_id = self.conf.trove_instance_id
        else:
            raise Exception("unknown backup type: %s" % self.conf.backup_media)

        restore_timestamp = None
        if self.conf.restore_from_date:
            restore_timestamp = utils.date_to_timestamp(self.conf.restore_from_date)
            backup = db.Backup.get_latest_backup(source_id, restore_timestamp)

        res = restore_service.RestoreOs(self.conf.client_manager,
                                        self.conf.container,
                                        self.storage)

        if backup_media == 'nova':
            backup = db.Backup.get_by_id(self.conf.nova_backup_id)
            if backup:
                restore_timestamp = backup.time_stamp
            else:
                raise ValueError("backup id does not exist".format(self.conf.nova_backup_id))
        elif backup_media == 'cindernative':
            backup = db.Backup.get_by_id(self.conf.cindernative_backup_id)
            if backup:
                restore_timestamp = backup.time_stamp
            else:
                raise ValueError("backup id does not exist".format(self.conf.cindernative_backup_id))
        elif backup_media == 'trove':
            backup = db.Backup.get_by_id(self.conf.trove_backup_id)
            if backup:
                restore_timestamp = backup.time_stamp
            else:
                raise ValueError("backup id does not exist".format(self.conf.trove_backup_id))
        if backup is not None:
            backup.status = db.BackupStatus.RESTORING
            backup.save()

        if backup_media == 'nova':
            if self.conf.is_rollback:
                LOG.info("Rollback nova backup. Instance ID: {0}, timestamp: {1} "
                         .format(self.conf.nova_inst_id, backup.time_stamp ))
                res.rollback_nova(self.conf.nova_inst_id, backup)
            elif self.conf.is_template:
                LOG.info("Create image from backup. Instance ID: {0}, timestamp: {1} "
                         .format(self.conf.nova_inst_id, backup.time_stamp))
                res.model_nova(self.conf.nova_inst_id, backup)
            else:
                LOG.info("Restoring nova backup. Instance ID: {0}, timestamp: {1} "
                         "network-id: {2}, backup-nova-name: {3}, "
                         "backup-flavor-id: {4}".format(self.conf.nova_inst_id,
                                                        backup.time_stamp,
                                                        self.conf.nova_restore_network,
                                                        self.conf.backup_nova_name,
                                                        self.conf.backup_flavor_id))

                res.restore_nova(self.conf.nova_inst_id, backup,
                                 self.conf.nova_restore_network,
                                 self.conf.backup_nova_name,
                                 self.conf.backup_flavor_id)
        elif backup_media == 'cindernative':
            LOG.info("Restoring cinder native backup. Volume ID {0}, Backup ID"
                     " {1}, Dest Volume ID {2}, timestamp: {3}".format(self.conf.cindernative_vol_id,
                                                                       self.conf.cindernative_backup_id,
                                                                       self.conf.cindernative_dest_id,
                                                                       restore_timestamp))
            res.restore_cinder(volume_id=self.conf.cindernative_vol_id,
                               backup_id=self.conf.cindernative_backup_id,
                               dest_volume_id=self.conf.cindernative_dest_id,
                               volume_type=self.conf.cindernative_volume_type,
                               restore_from_timestamp=restore_timestamp)
        elif backup_media == 'trove':
            LOG.info("Restoring cinde backup. Instance ID {0}, Backup ID"
                     " {1},  timestamp: {2}".format(self.conf.trove_instance_id,
                                                    self.conf.trove_backup_id,
                                                    restore_timestamp))
            res.restore_trove(self.conf.trove_instance_id,
                              self.conf.trove_backup_id,
                              restore_timestamp)
        else:
            raise Exception("unknown backup type: %s" % self.conf.backup_media)
        if backup is not None:
            backup.status = db.BackupStatus.AVAILABLE
            backup.save()
            return backup.to_primitive()
        else:
            return {}


class AdminJob(Job):

    def _validate(self):
        # no validation required in this job
        if not self.conf.remove_from_date and \
                not self.conf.remove_older_than and\
                not self.conf.nova_backup_id and \
                not self.conf.cindernative_backup_id and \
                not self.conf.trove_backup_id:
            raise ValueError("You need to provide to remove backup older "
                             "than this time. You can use --remove-older-than "
                             "or --remove-from-date")

    def execute(self):
        timestamp = None
        if self.conf.remove_from_date:
            timestamp = self.conf.remove_from_date
        elif self.conf.remove_older_than:
            timestamp = datetime.datetime.now() - \
                datetime.timedelta(days=self.conf.remove_older_than)
            timestamp = int(time.mktime(timestamp.timetuple()))

        admin_os = admin_service.AdminOs(self.conf.client_manager,
                                         self.conf.container,
                                         self.storage)
        backup_media = self.conf.mode
        if backup_media == 'nova':
            LOG.info('Executing nova admin. Instance ID: {0}'.format(
                self.conf.source_id))
            backups = admin_os.admin_nova(timestamp, backup_id=self.conf.nova_backup_id)
            size = 0
            for backup in backups:
                size += backup.size
            QUOTA.rollback(len(backups), size)

        elif backup_media == 'cindernative':
            LOG.info('Executing cinder native admin. Volume ID: {0}'
                     .format(self.conf.source_id))
            admin_os.admin_cinder(self.conf.source_id,
                                  self.conf.backend_id)
        elif backup_media == 'trove':
            LOG.info('Executing trove admin. Instance ID: {0}'.format(
                self.conf.source_id))
            admin_os.admin_trove(self.conf.source_id,
                                 self.conf.backend_id)
        else:
            raise Exception("unknown admin type: %s" % self.conf.backup_media)
        return {}


class ExecJob(Job):

    def _validate(self):
        if not self.conf.command:
            raise ValueError("--command option is required")

    def execute(self):
        if self.conf.command:
            LOG.info('Executing exec job. Command: {0}'
                     .format(self.conf.command))
            exec_cmd.execute(self.conf.command)
        else:
            LOG.warning(
                'No command info options were set. Exiting.')
        return {}
