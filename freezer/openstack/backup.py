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
        :return:
        """
        instance_id = instance_id
        client_manager = self.client_manager
        nova = client_manager.get_nova()
        instance = nova.servers.get(instance_id)
        glance = client_manager.get_glance()

        def instance_finish_task():
            instance = nova.servers.get(instance_id)
            return not instance.__dict__['OS-EXT-STS:task_state']

        utils.wait_for(instance_finish_task, 5, 300,
                       message="Wait for instance {0} to finish {1} to start snapshot "
                               "process".format(instance_id,
                                                instance.__dict__['OS-EXT-STS:task_state']))

        image_id = nova.servers.create_image(instance,
                                             "snapshot_of_%s" % instance_id)
        def image_active():
            image = glance.images.get(image_id)
            return image.status == 'active'

        utils.wait_for(image_active, 5, 300,
                       message="Wait for instance {0} "
                                "snapshot {1} to become active".format(instance_id, image_id))
        try:
            image = glance.images.get(image_id)
        except Exception as e:
            LOG.error(e)

        stream = client_manager.download_image(image, self.storage.max_segment_size)
        package = "{0}/{1}".format(instance_id, utils.DateTime.now().timestamp)
        if self.storage.type == "ceph":
            package = "{0}_{1}".format(instance_id, utils.DateTime.now().timestamp)
        LOG.info("Uploading image to %s", self.storage.type)
        headers = {"x-object-meta-name": instance.name,
                   "x-object-backup-name": name,
                   "x-object-meta-flavor-id": str(instance.flavor.get('id')),
                   'x-object-meta-length': str(len(stream))}
        self.storage.add_stream(stream, package, headers)
        LOG.info("Deleting temporary image {0}".format(image))
        glance.images.delete(image.id)

    def backup_cinder_by_glance(self, volume_id):
        """
        Implements cinder backup:
            1) Gets a stream of the image from glance
            2) Stores resulted image to the swift as multipart object

        :param volume_id: id of volume for backup
        """
        client_manager = self.client_manager
        cinder = client_manager.get_cinder()

        volume = cinder.volumes.get(volume_id)
        LOG.debug("Creation temporary snapshot")
        snapshot = client_manager.provide_snapshot(
            volume, "backup_snapshot_for_volume_%s" % volume_id)
        LOG.debug("Creation temporary volume")
        copied_volume = client_manager.do_copy_volume(snapshot)
        LOG.debug("Creation temporary glance image")
        image = client_manager.make_glance_image(copied_volume.id,
                                                 copied_volume)
        LOG.debug("Download temporary glance image {0}".format(image.id))
        stream = client_manager.download_image(image, self.storage.max_segment_size)
        package = "{0}/{1}".format(volume_id, utils.DateTime.now().timestamp)
        if self.storage.type == "ceph":
            package = "{0}_{1}".format(volume_id, utils.DateTime.now().timestamp)
        LOG.debug("Uploading image to %s", self.storage.type)
        headers = {'x-object-meta-length': str(len(stream)),
                   'volume_name': volume.name,
                   'volume_type': volume.volume_type,
                   'availability_zone': volume.availability_zone
                   }
        attachments = volume._info['attachments']
        if attachments:
            headers['server'] = attachments[0]['server_id']
        self.storage.add_stream(stream, package, headers=headers)
        LOG.debug("Deleting temporary snapshot")
        client_manager.clean_snapshot(snapshot)
        LOG.debug("Deleting temporary volume")
        cinder.volumes.delete(copied_volume)
        LOG.debug("Deleting temporary image")
        client_manager.get_glance().images.delete(image.id)

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
            backup.is_incremental = incremental
            backup.save()

        if incremental:
            trove.volume_backups.create(instance, name, description, container,
                                        incremental=True)
        else:
            trove.volume_backups.create(instance, name, description, container,
                                 incremental=False)
