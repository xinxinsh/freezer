import json
import os
import time
from oslo_log import log
from freezer.utils import utils

LOG = log.getLogger(__name__)

class AdminOs(object):
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

    def admin_nova(self,remove_older_timestamp, name=None):
        nova_key = CephStorage.backup_nova_name_pattern()
        backups = self.listdir(self.ceph_backup_pool)
        backups = filter(lambda x: re.search(nova_key, x), backups)
        for backup in backups:
            with RADOSClient(self) as client:
                vol = VolumeMetadataBackup(client, backup)
                vol_meta = vol.get()
            backup_name = None
            if vol_meta:
                backup_name = jsonutils.loads(vol_meta)['x-object-backup-name']
            timestamp = backup.rsplit('_', 1)[-1]
            if int(remove_older_timestamp) >= int(timestamp) \
                and backup_name == split[1]:
                LOG.debug("Deleting backup for volume %s.", backup)
                with RADOSClient(self) as client:
                    self.rbd.RBD().remove(client.ioctx, backup)

    def admin_cinder(self, volume_id=None, backup_id=None,
                     restore_from_timestamp=None):

        """
        Restoring cinder backup using
        :param volume_id:
        :param backup_id:
        :param restore_from_timestamp:
        :return:
        """

        cinder = self.client_manager.get_cinder()
        search_opts = {
            'volume_id': volume_id
        }
        backups = cinder.backups.list(search_opts=search_opts, sort='created_at')
        if not backup_id:
            def get_backups_from_timestamp(backups, restore_from_timestamp):
                for backup in backups:
                    backup_created_date = backup.created_at.split('.')[0]
                    backup_created_timestamp = utils.utc_to_local_timestamp(backup_created_date)
                    if backup_created_timestamp <= restore_from_timestamp:
                        yield backup

            backups_filter = get_backups_from_timestamp(backups,
                                                        restore_from_timestamp)
            if not backups_filter:
                LOG.warning("no available backups for cinder volume,"
                            "admin  backup")
                delete_backup_start = max(backups, key=lambda x: x.created_at)
            else:
                delete_backup_start = min(backups_filter, key=lambda x: x.created_at)
        else:
            delete_backup_start = None
            for backup in backups:
                if backup_id == backup.id:
                    delete_backup_start = backup
        if delete_backup_start:
            delete_start_time = delete_backup_start.created_at
            delete_base_name = delete_backup_start.description
            for backup in backups:
                if backup.created_at >= delete_start_time and \
                            backup.description == delete_base_name:
                    LOG.debug("Deleting cindernative backup %s.", backup.id)
                    cinder.backups.delete(backup.id)
                    time.sleep(2)

    def admin_trove(self, instance=None, backup_id=None,
                    restore_from_timestamp=None):

        """
        Admin trove backup using
        :param instance:
        :param backup_id:
        :param restore_from_timestamp:
        :return:
        """

        client_manager = self.client_manager
        trove = client_manager.get_trove()
        backups = trove.instances.volume_backups(instance)
        backups.sort(key=lambda x: x['created'],reverse=True)

        if not backup_id:
            def get_backups_from_timestamp(backups, restore_from_timestamp):
                for backup in backups:
                    backup_created_date = backup.created.split('.')[0]
                    backup_created_timestamp = utils.utc_to_local_timestamp(backup_created_date)
                    if backup_created_timestamp <= restore_from_timestamp:
                        yield backup

            backups_filter = get_backups_from_timestamp(backups,
                                                        restore_from_timestamp)
            if not backups_filter:
                LOG.warning("no available backups for trove,"
                            "admin  backup")
                delete_backup_start = max(backups, key=lambda x: x.created)
            else:
                delete_backup_start = min(backups_filter, key=lambda x: x.created)

        else:
            delete_backup_start = None
            for backup in backups:
                if backup_id == backup.id:
                    delete_backup_start = backup
        if delete_backup_start:
            delete_start_time = delete_backup_start.created
            for backup in backups:
                if backup.created >= delete_start_time:
                    LOG.debug("Deleting trove backup %s.", backup.id)
                    trove.volume_backups.delete(backup.id)
                    time.sleep(2)

