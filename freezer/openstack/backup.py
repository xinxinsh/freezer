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

    def backup_nova(self, instance_id, name=None, backup=None):
        """
        Implement nova backup
        :param instance_id: Id of the instance for backup
        :param name: name of this backup
        :param backup: backup dict
        :return:
        """
        instance_id = instance_id
        client_manager = self.client_manager
        nova = client_manager.get_nova()
        instance = nova.servers.get(instance_id)

        connection_info = nova.servers.connection_info(instance_id)._info
        if backup is not None:
            backup.source_id = instance_id
            #dummy value, pls, revise this value properly
            backup.is_incremental = False
            backup.save()

        nova.servers.update_task(instance_id, 'image_backuping')
        headers = {"x-object-meta-name": instance.name,
                   "x-object-backup-name": name,
                   "x-object-meta-flavor-id": str(instance.flavor.get('id'))}
        if self.storage.type == 'ceph' and connection_info['driver_volume_type'] == 'rbd':
            package = "{0}_{1}".format(instance_id, utils.DateTime.now().timestamp)
            self.storage.backup(connection_info, package, headers)
        else:
            # should do full backup
            pass
        nova.servers.update_task(instance_id, None, 'image_backuping')

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
            backup.is_incremental = incremental
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
            backup.is_incremental = incremental
            backup.save()

        if incremental:
            trove.volume_backups.create(instance, name, description, container,
                                        incremental=True)
        else:
            trove.volume_backups.create(instance, name, description, container,
                                        incremental=False)
