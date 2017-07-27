"""
(c) Copyright 2014,2015 Hewlett-Packard Development Company, L.P.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Freezer Backup modes related functions
"""
import time

from oslo_log import log

from freezer.utils import utils
from freezer.exceptions import utils as ex_utils
from freezer.utils import backup as db_backup

LOG = log.getLogger(__name__)


class BackupOs(object):

    def __init__(self, client_manager, container, storage):
        """

        :param client_manager:
        :param container:
        :param storage:
        :type storage: freezer.swift.SwiftStorage
        :return:
        """
        self.client_manager = client_manager
        self.container = container
        self.storage = storage

    def backup_nova(self, instance_id, name=None, incremental=True, backup=None):
        """
        Implement nova backup
        :param instance_id: Id of the instance for backup
        :param name: name of this backup
        :return:
        """
        instance_id = instance_id
        client_manager = self.client_manager
        nova = client_manager.get_nova()
        instance = nova.servers.get(instance_id)
        if backup:
            backup_id = backup.backup_id

        def instance_finish_task():
            instance = nova.servers.get(instance_id)
            return not instance.__dict__['OS-EXT-STS:task_state']

        utils.wait_for(instance_finish_task, 5, 300,
                       message="Wait for instance {0} to finish {1} to start snapshot "
                               "process".format(instance_id,
                                                instance.__dict__['OS-EXT-STS:task_state']))

        connection_info = nova.servers.connection_info(instance_id)._info
        nova_volume_type = connection_info['driver_volume_type'] 
        if incremental and (self.storage.type != 'ceph' or nova_volume_type != 'rbd'):
            raise ex_utils.NotSupportException("Does Not Support Incremental Backup")

        if backup is not None:
            backup.source_id = instance_id
            backup.save()

        nova.servers.update_task(instance_id, 'image_backuping')
        package = "{0}_{1}_{2}".format(instance_id, backup_id, backup.time_stamp)

        LOG.debug("Creation {0} backup".format('INCREMENTAL' if incremental else 'FULL'))

        headers = {"x-object-meta-name": instance.name,
                   "x-object-backup-name": name,
                   "x-object-meta-flavor-id": str(instance.flavor.get('id'))}
        latest_backup = db_backup.Backup.get_latest_backup(source_id=instance_id)
        if incremental and latest_backup:
            backup.backup_chain_name = latest_backup.backup_chain_name
            backup.parent_id = latest_backup.backup_id
        else:
            backup.backup_chain_name = backup.backup_id
            backup.parent_id = None
        backup.source_id = instance_id
        info = self.storage.backup(connection_info, package, headers, backup)

        nova.servers.update_task(instance_id, None, 'image_backuping')


        return info

    def backup_cinder(self, volume_id, name=None, description=None,
                      incremental=True, backup=None):
        client_manager = self.client_manager
        cinder = client_manager.get_cinder()
        container = "{0}/{1}/{2}".format(self.container, volume_id,
                                         utils.DateTime.now().timestamp)
        if incremental:
            search_opts = {
                'volume_id': volume_id,
                'status': 'available'
            }
            backups = cinder.backups.list(search_opts=search_opts)
            if len(backups) <= 0:
                LOG.info("No backups exists for volume %s ."
                         "Degrade to do a full backup before do incremental backup"
                         % volume_id)
                incremental = False

        if backup is not None:
            backup.source_id = volume_id
            backup.save()

        if incremental:
            backup_meta = cinder.backups.create(volume_id, container, name, description,
                                                incremental=True, force=True)
        else:
            cinder.volumes.set_metadata(volume_id, {'backup_chain_name': name})
            backup_meta = cinder.backups.create(volume_id, container, name, description,
                                                incremental=False, force=True)
        backup_volumes = cinder.volumes.get(volume_id)
        backup_meta._info['backup_chain_name'] = backup_volumes.metadata['backup_chain_name']
        return backup_meta._info

    def backup_trove(self, instance, name, description=None,
                     incremental=True, backup=None):

        client_manager = self.client_manager
        trove = client_manager.get_trove()
        container = "{0}/{1}/{2}".format(self.container, instance,
                                         utils.DateTime.now().timestamp)

        if incremental:
            backups = trove.instances.volume_backups(instance)
            if len(backups) <= 0:
                msg = ("No backups exists for instance %s ."
                       "Degrade to do a full backup before do incremental backup"
                       % instance)
                LOG.info(msg)
                incremental = False

        if backup is not None:
            backup.source_id = instance
            backup.save()

        if incremental:
            trove.volume_backups.create(instance, name, description, container,
                                        incremental=True)
        else:
            trove.volume_backups.create(instance, name, description, container,
                                        incremental=False)
