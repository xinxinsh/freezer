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

"""

import json
import io
import time
import re
import datetime

from oslo_log import log
from oslo_serialization import jsonutils
from oslo_utils import units
from freezer.storage import physical
from freezer.utils import utils as freezer_utils
from freezer.storage import exceptions
from freezer import _, _LW

try:
    import rbd
    import rados
except ImportError:
    rbd = None
    rados = None

LOG = log.getLogger(__name__)

class CephStorage(physical.PhysicalStorage):

    """Backup Cinder volumes and Instance disks to Ceph a RBD Image
    This class enables backing up Cinders volumes to Ceph a RBD Image.
    Backups may be stored in their own pool or even cluster.
    """

    _type = 'ceph'

    def __init__(self, client_manager, pool, max_segment_size, skip_prepare=True):
        self.rbd = rbd
        self.rados = rados
        self.client_manager = client_manager

        self.rbd_stripe_count = 0
        self.rbd_stripe_unit = 0
        self.chunk_size = max_segment_size

        self.ceph_backup_user = freezer_utils.convert_str("admin")
        self.ceph_backup_pool = freezer_utils.convert_str(pool)
        self.ceph_backup_conf = freezer_utils.convert_str("/etc/ceph/ceph.conf")
        super(CephStorage, self).__init__(
            storage_path=pool,
            max_segment_size=max_segment_size,
            skip_prepare=skip_prepare)

    def prepare(self):
        pass

    def backup_blocks(self, backup):
        pass

    def create_dirs(self, path):
        pass

    def get_file(self, from_path, to_path):
        pass

    def put_file(self, from_path, to_path):
        pass

    def write_backup(self, rich_queue, backup):
        pass

    def _connect_to_rados(self, pool=None):
        """Establish connection to the backup Ceph cluster"""
        client = self.rados.Rados(rados_id=self.ceph_backup_user,
                                  conffile=self.ceph_backup_conf)
        try:
            client.connect()
            pool_to_open = freezer_utils.convert_str(pool or self.ceph_backup_pool)
            ioctx = client.open_ioctx(pool_to_open)
            return client, ioctx
        except self.rados.Error:
            # shutdown cannot raise an exception
            client.shutdown()
            raise

    def _disconnect_from_rados(self, client, ioctx):
        """Terminate connection with the backup Ceph cluster"""
        # close an ioctx cannot raise an exception
        ioctx.close()
        client.shutdown()

    def _get_image_size_gb(self, size):
        """Return the size in gigabytes of the given volume.
        """
        if int(size) == 0:
            msg = _("Need non-zero volume size")
            raise exceptions.InvalidParameterValue(msg)
        return int(size) * units.Gi

    def _upload_chunks(self, stream, dest, length):
        """Transfer data between files(Python IO Objects)"""

        chunks = int(length / stream.chunk_size)
        LOG.debug("%(chunks)s chunks of %(bytes)s bytes to be transferred",
                  {'chunks': chunks, 'bytes':stream.chunk_size})

        chunk = 0
        for el in stream:
            before = time.time()

            dest.write(el)
            dest.flush()
            delta = (time.time() - before)
            rate = (stream.chunk_size / delta) / 1024
            LOG.debug("Transferred chunk %(chunk)s of %(chunks)s "
                      "(%(rate)dK/s)",
                    {'chunk': chunk + 1,
                     'chunks': chunks,
                     'rate': rate})
            chunk += 1

    def add_stream(self, stream, backup_name, headers=None):

        length = stream.length
        volume_id = backup_name.rsplit("_", 1)[0]

        with RADOSClient(self, self.ceph_backup_pool) as client:
            # create base image
            old_format, features = self._get_rbd_support()
            LOG.debug("Creating backup base image='%(name)s' for volume/instance "
                      "%(volume)s.",
                      {'name':backup_name, 'volume':volume_id})
            self.rbd.RBD().create(ioctx=client.ioctx,
                                  name=backup_name,
                                  size=length,
                                  old_format=old_format,
                                  features=features,
                                  stripe_unit=self.rbd_stripe_unit,
                                  stripe_count=self.rbd_stripe_count)

            LOG.debug("Copying data from volume %s.", volume_id)
            dest_rbd = self.rbd.Image(client.ioctx, backup_name)
            try:
                rbd_meta = RBDImageMetadata(dest_rbd,
                                            self.ceph_backup_pool,
                                            self.ceph_backup_user,
                                            self.ceph_backup_conf)
                rbd_fd = RBDImageIOWrapper(rbd_meta)
                LOG.debug("Transferring data between '%(src)s' and '%(dest)s'",
                          {'src':volume_id, 'dest':backup_name})
                self._upload_chunks(stream, rbd_fd, length)
            finally:
                dest_rbd.close()

        if not headers:
            headers = {}
        headers['X-Object-Manifest'] = u'{0}/{1}'.format(
            self.ceph_backup_pool, backup_name)

        json_meta = jsonutils.dumps(headers)
        LOG.debug("Backing up metadata for volume %s.", volume_id)
        try:
            with RADOSClient(self) as client:
                vol_meta_backup = VolumeMetadataBackup(client, backup_name)
                vol_meta_backup.set(json_meta)
        except exceptions.VolumeMetadataBackupExists as e:
            msg = (_("Failed to backup volume metadata - %s") % e)
            raise exceptions.BackupOperationError(msg)

    def get_header(self, backup_name):
        """Get backup metdata"""
        with RADOSClient(self, self.ceph_backup_pool) as client:
            vol_meta = VolumeMetadataBackup(client, backup_name)
            json_meta = vol_meta.get()
            header = jsonutils.loads(json_meta)

        return header

    def get_backups(self, pool, prefix):
        """Return RBD Images prefix with {prefix}"""
        with RADOSClient(self, pool) as client:
            backups = self.rbd.RBD().list(client.ioctx)
            backups = list(filter(lambda x: x.rsplit("_", 1)[0] == prefix, backups))

        return backups

    def create_image(self, backup_name):
        with RADOSClient(self, self.ceph_backup_pool) as client:
            LOG.debug("Open rbd image %s/%s", self.ceph_backup_pool, backup_name)
            src_rbd = self.rbd.Image(client.ioctx, backup_name)
            try:
                rbd_meta = RBDImageMetadata(src_rbd,
                                            self.ceph_backup_pool,
                                            self.ceph_backup_user,
                                            self.ceph_backup_conf)
                rbd_fd = RBDImageIOWrapper(rbd_meta)
                path = "{0}/{1}".format(self.ceph_backup_pool, backup_name)
                image = self.client_manager.create_image(
                    name="restore_{}".format(path),
                    container_format="bare",
                    disk_format="raw",
                    data=rbd_fd)
            finally:
                src_rbd.close()

        return image

    def info(self):
        with RADOSClient(self, self.ceph_backup_pool) as client:
            backups = self.rbd.RBD().list(client.ioctx)
            ordered_backups = {}
            for backup in backups:
                print(backup)
                try:
                    image = self.rbd.Image(client.ioctx, backup)
                    info = image.stat()
                    ordered_backups['image_name'] = "{0}/{1}".format(self.ceph_backup_pool, backup)
                    size = '{0}'.format((int(info['size']) / 1024) / 1024)
                    if size == '0':
                        size = '1'
                    ordered_backups['size'] = '{0}MB'.format(size)
                    ordered_backups['objects_count'] = info['num_objs']
                    print(json.dumps(
                        ordered_backups, indent=4,
                        separators=(',', ': '), sort_keys=True))
                finally:
                    image.close()

    @staticmethod
    def backup_cindernative_name_pattern():
        """Returns the pattern used to match cindernative backups"""
        return r"^volume\.([a-z0-9\-]+?)\.backup\.([a-z0-9\-]+?)$"

    @staticmethod
    def backup_nova_name_pattern():
        """Returns the pattern used to match cinder or nova backups"""
        return r"^([a-z0-9\-]+?)_([0-9]+?)$"

    def remove_older_than(self, engine, remove_older_timestamp,
                          hostname_backup_name):
        """Remove backups older than remove_older_timestamp"""
        LOG.debug("Delete started for backups older than %s.",
                  datetime.datetime.fromtimestamp(remove_older_timestamp))
        cinder = self.client_manager.get_cinder()
        nova_key = CephStorage.backup_nova_name_pattern()
        backups = self.listdir(self.ceph_backup_pool)
        backups = filter(lambda x: re.search(nova_key, x), backups)
        for backup in backups:
            timestamp = backup.rsplit('_', 1)[-1]
            if int(remove_older_timestamp) >= int(timestamp):
                LOG.debug("Deleting backup for volume %s.", backup)
                with RADOSClient(self) as client:
                    self.rbd.RBD().remove(client.ioctx, backup)

                LOG.debug("Removing metadata object for volume %s.", backup)
                with RADOSClient(self) as client:
                    VolumeMetadataBackup(client, backup).remove_if_exists()

        backups = cinder.backups.list()
        for backup in backups:
            backup_created_date = backup.created_at.split('.')[0]
            backup_created_timestamp = freezer_utils.utc_to_local_timestamp(backup_created_date)
            if int(remove_older_timestamp) >= int(backup_created_timestamp):
                LOG.debug("Deleting cindernative backup %s.", backup.id)
                cinder.backups.delete(backup)

    def listdir(self, path):
        """Return RBD Images prefix with {prefix}"""
        with RADOSClient(self, path) as client:
            backups = self.rbd.RBD().list(client.ioctx)

        return backups

    def rmtree(self, backup):
        pass

    @property
    def _supports_layring(self):
        """Determine if copy-on-write is supported by our version of librbd"""
        return hasattr(self.rbd, 'RBD_FEATURE_LAYRING')

    @property
    def _supports_stripingv2(self):
        """Determine if stripping is support by our version of librbd"""
        return hasattr(self.rbd, 'RBD_FEATURE_STRIPINGV2')

    def _get_rbd_support(self):
        """Determine RBD features supported by our version of librbd"""
        old_format = True
        features = 0
        if self._supports_layring:
            old_format = False
            features |= self.rbd.RBD_FEATURE_LAYRING
        if self._supports_stripingv2:
            old_format = False
            features |= self.rbd.RBD_FEATURE_STRIPINGV2

        return (old_format, features)


class RADOSClient(object):
    """Context manager to simplify error handling for connecting to ceph."""
    def __init__(self, driver, pool=None):
        self.driver = driver
        self.cluster, self.ioctx = driver._connect_to_rados(pool)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver._disconnect_from_rados(self.cluster, self.ioctx)

    @property
    def features(self):
        features = self.cluster.conf_get('rbd_default_features')
        if ((features is None) or (int(features) == 0)):
            features = self.driver.rbd.RBD_FEATURE_LAYERING
        return int(features)

class RBDImageMetadata(object):
    """RBD image metadata to be used with RBDImageIOWrapper."""
    def __init__(self, image, pool, user, conf):
        self.image = image
        self.pool = freezer_utils.convert_str(pool)
        self.user = freezer_utils.convert_str(user)
        self.conf = freezer_utils.convert_str(conf)

class RBDImageIOWrapper(io.RawIOBase):
    """Enables LibRBD.Image objects to be treated as Python IO objects.

    Calling unimplemented interfaces will raise IOError.
    """

    def __init__(self, rbd_meta):
        super(RBDImageIOWrapper, self).__init__()
        self._rbd_meta = rbd_meta
        self._offset = 0
    def _inc_offset(self, length):
        self._offset += length

    @property
    def rbd_image(self):
        return self._rbd_meta.image

    @property
    def rbd_user(self):
        return self._rbd_meta.user

    @property
    def rbd_pool(self):
        return self._rbd_meta.pool

    @property
    def rbd_conf(self):
        return self._rbd_meta.conf


    def read(self, length=None):
        offset = self._offset
        total = self._rbd_meta.image.size()

        # NOTE(dosaboy): posix files do not barf if you read beyond their
        # length (they just return nothing) but rbd images do so we need to
        # return empty string if we have reached the end of the image.
        if (offset >= total):
            return b''

        if length is None:
            length = total

        if (offset + length) > total:
            length = total - offset

        self._inc_offset(length)
        return self._rbd_meta.image.read(int(offset), int(length))


    def write(self, data):
        self._rbd_meta.image.write(data, self._offset)
        self._inc_offset(len(data))


    def seekable(self):
        return True


    def seek(self, offset, whence=0):
        if whence == 0:
            new_offset = offset
        elif whence == 1:
            new_offset = self._offset + offset
        elif whence == 2:
            new_offset = self._rbd_meta.image.size()
            new_offset += offset
        else:
            raise IOError(_("Invalid argument - whence=%s not supported") %
                          (whence))

        if (new_offset < 0):
            raise IOError(_("Invalid argument"))

        self._offset = new_offset


    def tell(self):
        return self._offset


    def flush(self):
        try:
            self._rbd_meta.image.flush()
        except AttributeError:
            LOG.warning(_LW("flush() not supported in "
                            "this version of librbd"))

    def fileno(self):
        """RBD does not have support for fileno() so we raise IOError.

        Raising IOError is recommended way to notify caller that interface is
        not supported - see http://docs.python.org/2/library/io.html#io.IOBase
        """
        raise IOError(_("fileno() not supported by RBD()"))

    # NOTE(dosaboy): if IO object is not closed explicitly, Python auto closes
    # it which, if this is not overridden, calls flush() prior to close which
    # in this case is unwanted since the rbd image may have been closed prior
    # to the autoclean - currently triggering a segfault in librbd.
    def close(self):
        pass

class VolumeMetadataBackup(object):

    def __init__(self, client, backup_name):
        self._client = client
        self._backup_name = backup_name

    @property
    def name(self):
        return freezer_utils.convert_str("%s.meta" % self._backup_name)

    @property
    def exists(self):
        meta_obj = rados.Object(self._client.ioctx, self.name)
        return self._exists(meta_obj)

    def _exists(self, obj):
        try:
            obj.stat()
        except rados.ObjectNotFound:
            return False
        else:
            return True

    def set(self, json_meta):
        """Write Json metadata to a new object.

        This should only be called once per backup. Raises
        VolumeMetadataBackupExists if the object already exists.
        """
        meta_obj = rados.Object(self._client.ioctx, self.name)
        if not self._exists(meta_obj):
            msg = _("Metadata backup object '%s' alreadly exists") % self.name
            raise exceptions.VolumeMetadataBackupExists(msg)

        meta_obj.write(json_meta)

    def get(self):
        """Get metadata backup object.

        Returns None if the object does not exists.
        """
        meta_obj = rados.Object(self._client.ioctx, self.name)
        if not self._exists(meta_obj):
            LOG.debug("Metadata bacup object %s does not exist", self.name)
            return None

        return meta_obj.read()

    def remove_if_exists(self):
        meta_obj = rados.Object(self._client.ioctx, self.name)
        try:
            meta_obj.remove()
        except rados.ObjectNotFound:
            LOG.debug("Metadata backup object '%s' not found - ignoring",
                      self.name)