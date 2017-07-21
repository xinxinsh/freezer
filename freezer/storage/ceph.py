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
import tempfile
import os
import subprocess
import fcntl

from oslo_log import log
from oslo_serialization import jsonutils
from oslo_utils import units
from oslo_utils import fileutils
from freezer.storage import physical
from freezer.utils import utils as freezer_utils
from freezer.storage import exceptions
from freezer import _, _LW, _LI

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

    def __init__(self, client_manager, backup_ceph_pool, backup_ceph_user,
                 backup_ceph_conf, backup_ceph_chunk_size, backup_ceph_stripe_count,
                 backup_ceph_stripe_unit, skip_prepare=True):
        self.rbd = rbd
        self.rados = rados
        self.client_manager = client_manager
        self.chunk_size = backup_ceph_chunk_size

        if self._supports_stripingv2:
            self.rbd_stripe_count = backup_ceph_stripe_count
            self.rbd_stripe_unit = backup_ceph_stripe_unit
        else:
            LOG.info(_LI("RBD striping not supported - ignoring configuration "
                         "settings for rbd striping"))
            self.rbd_stripe_count = 0
            self.rbd_stripe_unit = 0

        self.ceph_backup_user = freezer_utils.convert_str(backup_ceph_user)
        self.ceph_backup_pool = freezer_utils.convert_str(backup_ceph_pool)
        self.ceph_backup_conf = freezer_utils.convert_str(backup_ceph_conf)
        super(CephStorage, self).__init__(
            storage_path=self.ceph_backup_pool,
            max_segment_size=self.chunk_size,
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

    def _connect_to_rados(self, pool=None, user=None, cluster=None, conf=None):
        """Establish connection to the backup Ceph cluster"""
        rbd_user = freezer_utils.convert_str(user or self.ceph_backup_user)
        rbd_clustername = freezer_utils.convert_str(cluster or "ceph")
        rbd_conf = freezer_utils.convert_str(conf or self.ceph_backup_conf)
        rbd_pool = freezer_utils.convert_str(pool or self.ceph_backup_pool)

        client = self.rados.Rados(rados_id=rbd_user,
                                  clustername=rbd_clustername,
                                  conffile=rbd_conf)
        try:
            client.connect()
            ioctx = client.open_ioctx(rbd_pool)
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

    def _backup_metadata(self, headers, backup_name):
        json_meta = jsonutils.dumps(headers)
        try:
            with RADOSClient(self) as client:
                vol_meta_backup = VolumeMetadataBackup(client, backup_name)
                vol_meta_backup.set(json_meta)
        except exceptions.VolumeMetadataBackupExists as e:
            msg = (_("Failed to backup volume metadata - %s") % e)
            raise exceptions.BackupOperationError(msg)
        
    def _get_snaps(self, rbd_image, sort=False):
        snaps = rbd_image.list_snaps()
        backup_snaps = []
        for snap in snaps:
            search_key = r"^([a-z0-9\-]+?)_snap_(.+)$"
            result = re.search(search_key, snap['name'])
            if result:
                backup_snaps.append({'name': result.group(0),
                                     'backup_id': result.group(1),
                                     'timestamp': result.group(2)})

        if sort:
            # Sort into ascending order of timestamp
            backup_snaps.sort(key=lambda x: x['timestamp'], reverse=True)

        return backup_snaps

    def _get_most_recent_snap(self, rbd_image):
        backup_snaps = self._get_snaps(rbd_image, sort=True)
        if not backup_snaps:
            return None

        return backup_snaps[0]['name']

    def _get_backup_base_name(self, backup_name, chain_name=None):
        if chain_name:
            return freezer_utils.convert_str("%s.backup.base.%s" % (backup_name, chain_name))
        return freezer_utils.convert_str("%s.backup.base" % (backup_name))

    def _snap_exist(self, backup_name , snap_name, client):
        base_image = self.rbd.Image(client.ioctx, backup_name)
        try:
            snaps = base_image.list_snaps()
        finally:
            base_image.close()

        if snaps is None:
            return False

        for snap in snaps:
            if snap['name'] == snap_name:
                return True

        return False

    def _create_ceph_conf(self, hosts, ports, keyring):
        monitors = ["%s:%s" % (ip, port) for ip, port in zip(hosts, ports)]
        mon_hosts = "mon_host = %s" % (','.join(monitors))
        try:
            fd, ceph_conf_path = tempfile.mkstemp(prefix="tmprbd_")
            with os.fdopen(fd, 'w') as conf_file:
         #       conf_file.writelines(["log file = /var/log/ceph/ceph-nova.log", "\n", "debug rbd = 30", "\n"])
                conf_file.writelines([mon_hosts, "\n", keyring, "\n"])
            return ceph_conf_path
        except IOError:
            msg = (_("Failed to write data to %s.") % (ceph_conf_path))
            raise exceptions.ConfException(msg) 

    def _get_new_snap_name(self, backup_name, timestamp=None):
        if not timestamp:
            timestamp = freezer_utils.DateTime.now().timestamp
        return freezer_utils.convert_str("%s_snap_%s" %(backup_name, timestamp))

    def _delete_base_image(self, backup_name, snap_name, client):
        """
        try to delete base image
        """
        delay = 5
        retries = 3
        image_exist = False

        rbds = self.rbd.RBD().list(client.ioctx)
        if backup_name in rbds:
            image_exist = True

        if not image_exist:
            raise self.rbd.ImageNotFound(_("image %s not found") % backup_name)

        base_image = self.rbd.Image(client.ioctx, base_name)
        while retries > 0:
            snap_exist = self._snap_exist(base_name, snap_name, client)
            if snap_exist:
                LOG.debug("Deleting backup Snapshot %s" % (snap_name))
                base_image.remove_snap(new_snap)
            else:
                LOG.debug("No backup snapshot to delete")

            backup_snaps = self._get_snaps(base_image)
            if backup_snaps:
                LOG.info(
                   _LI("Backup base image of volume %(volume)s still "
                       "has %(snapshots)s snapshots so skipping base "
                       "image delete."),
                    {'snapshots': len(backup_snaps), 'volume': backup_name})
                base_image.close()
                return

            LOG.info(_LI("Deleting backup base image='%(basename)s'"),
                         {'basename': base_name})
                        
            try:
                self.rbd.RBD().remove(client.ioctx, base_name)
            except rbd.ImageBusy:
                if retries > 0:
                    LOG.info(_LI("Backup image is "
                                 "busy, retrying %(retries)s more time(s) "
                                 "in %(delay)ss."),
                             {'retries': retries,
                              'delay': delay})
                    eventlet.sleep(delay)
                else:
                    LOG.error(_LE("Max retries reached deleting backup "
                                  "%(basename)s image "),
                              {'basename': base_name})
                    raise
            else:
                LOG.debug("Base backup image='%(basename)s'",
                          {'basename': base_name})
                retries = 0
            finally:
                retries -= 1
        base_image.close()

    def _create_base_image(self, base_name, size, client):
        """
        create a base image
        """
        LOG.debug("Creating base image %s", base_name)       
        old_format, features = self._get_rbd_support()
        self.rbd.RBD().create(ioctx=client.ioctx,
                                  name=base_name,
                                  size=size,
                                  old_format=old_format,
                                  features=features,
                                  stripe_unit=self.rbd_stripe_unit,
                                  stripe_count=self.rbd_stripe_count)
        
    def _ceph_args(self, user, conf=None, pool=None):
        """Create default ceph args for executing rbd commands.

        If no --conf is provided, rbd will look in the default locations e.g.
        /etc/ceph/ceph.conf
        """
        args = ['--id', user]
        if conf:
            args.extend(['--conf', conf])
        if pool:
            args.extend(['--pool', pool])

        return args

    def _piped_execute(self, cmd1, cmd2):
        """Pipe output of cmd1 into cmd2."""
        LOG.debug("Piping cmd1='%s' into...", ' '.join(cmd1))
        LOG.debug("cmd2='%s'", ' '.join(cmd2))

        try:
            p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        except OSError as e:
            LOG.error(_LE("Pipe1 failed - %s "), e)
            raise

        # NOTE(dosaboy): ensure that the pipe is blocking. This is to work
        # around the case where evenlet.green.subprocess is used which seems to
        # use a non-blocking pipe.
        flags = fcntl.fcntl(p1.stdout, fcntl.F_GETFL) & (~os.O_NONBLOCK)
        fcntl.fcntl(p1.stdout, fcntl.F_SETFL, flags)

        try:
            p2 = subprocess.Popen(cmd2, stdin=p1.stdout,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        except OSError as e:
            LOG.error(_LE("Pipe2 failed - %s "), e)
            raise

        p1.stdout.close()
        stdout, stderr = p2.communicate()
        return p2.returncode, stderr
    
    def _rbd_diff_transfer(self, src_name, src_pool, dest_name, dest_pool,
                           src_user, src_conf, dest_user, dest_conf,
                           src_snap=None, from_snap=None):
        """Copy only extents changed between two points.

        If no snapshot is provided, the diff extents will be all those changed
        since the rbd volume/base was created, otherwise it will be those
        changed since the snapshot was created.
        """
        LOG.debug("Performing differential transfer from '%(src)s' to "
                  "'%(dest)s'",
                  {'src': src_name, 'dest': dest_name})

        # NOTE(dosaboy): Need to be tolerant of clusters/clients that do
        # not support these operations since at the time of writing they
        # were very new.

        src_ceph_args = self._ceph_args(src_user, src_conf, pool=src_pool)
        dest_ceph_args = self._ceph_args(dest_user, dest_conf, pool=dest_pool)

        cmd1 = ['rbd', 'export-diff'] + src_ceph_args
        if from_snap is not None:
            cmd1.extend(['--from-snap', from_snap])
        if src_snap:
            path = freezer_utils.convert_str("%s/%s@%s"
                                     % (src_pool, src_name, src_snap))
        else:
            path = freezer_utils.convert_str("%s/%s" % (src_pool, src_name))
        cmd1.extend([path, '-'])

        cmd2 = ['rbd', 'import-diff'] + dest_ceph_args
        rbd_path = freezer_utils.convert_str("%s/%s" % (dest_pool, dest_name))
        cmd2.extend(['-', rbd_path])

        ret, stderr = self._piped_execute(cmd1, cmd2)
        if ret:
            msg = (_("RBD diff op failed - (ret=%(ret)s stderr=%(stderr)s)") %
                   {'ret': ret, 'stderr': stderr})
            LOG.info(msg)
            raise exceptions.BackupOperationError(msg)

    def backup(self, connection_info, backup_name, headers=None, backup=None):
        src_conn = connection_info['data']
        src_pool, src_name = src_conn['name'].rsplit('/', 1)
        src_user = src_conn['auth_username']
        src_cluster = src_conn['cluster_name']
        src_conf = self._create_ceph_conf(src_conn['hosts'], src_conn['ports'],
                                          src_conn['keyring'])
        src_client = RADOSClient(self, src_pool, src_user, src_cluster, src_conf)
        src_image = self.rbd.Image(src_client.ioctx, freezer_utils.convert_str(src_name))
        src_size = src_image.size()
        backup.size = src_size
        backup_base = backup_name.rsplit("_", 2)[0]
        backup_id = backup.backup_id
        timestamp = backup.time_stamp
        chain_name = backup.backup_chain_name
        dst_client = RADOSClient(self, self.ceph_backup_pool)
        
        from_snap = self._get_most_recent_snap(src_image)
        base_name = self._get_backup_base_name(backup_base, chain_name)
        image_created = False

        if base_name not in self.rbd.RBD().list(ioctx=dst_client.ioctx):
            if from_snap:
                LOG.debug("source snapshot {0} of volume {1} is stale or deleting", from_snap, src_name)
                src_image.remove_snap(from_snap)
                from_snap = None
            self._create_base_image(base_name, src_size, dst_client)
            image_created = True
        else:
            if not self._snap_exist(src_name, from_snap, src_client):
                errmsg = (_("Snapshot='%(snap)s' does not exist in base "
                            "image='%(base)s' - aborting incremental "
                             "backup") %
                          {'snap': from_snap, 'base': base_name})
                LOG.info(errmsg)
                raise exceptions.BackupOperationError(errmsg)

        new_snap = self._get_new_snap_name(backup_id, timestamp)
        src_image.create_snap(new_snap)
        try:
            self._rbd_diff_transfer(src_name, src_pool, base_name, 
                                    self.ceph_backup_pool,
                                    src_user=src_user, src_conf=src_conf,
                                    dest_user=self.ceph_backup_user,
                                    dest_conf=self.ceph_backup_conf,
                                    from_snap=from_snap, src_snap=new_snap)
            LOG.debug("Differential backup transfer compliete")

            if from_snap:
                src_image.remove_snap(from_snap)

        except exceptions.BackupOperationError:
            LOG.debug("Differential backup transfer failed")
            if image_created:
                self._delete_base_image(base_name, new_snap)

            LOG.debug("Deleting diff backup snapshot %s from source volume" %(new_snap))
            src_image.remove_snap(new_snap)
            fileutils.delete_if_exists(src_conf)

        if image_created:
	    if not headers:
		headers = {}
	    headers['X-Object-Manifest'] = u'{0}/{1}'.format(
		self.ceph_backup_pool, base_name)
	    headers["x-object-meta-length"] = src_size
	    
            base_name = self._get_backup_base_name(backup_base)
	    self._backup_metadata(headers, base_name)

        return backup

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
        
        self._backup_metadata(headers, backup_name)

    def get_header(self, backup):
        """Get backup metdata"""
        base_id = backup.source_id
        chain_name = backup.backup_chain_name
        backup_name = '{0}_{1}_{2}'.format(base_id, backup.backup_id, backup.time_stamp)
        with RADOSClient(self, self.ceph_backup_pool) as client:
            base_name = self._get_backup_base_name(base_id, chain_name)
            backups = self.rbd.RBD().list(client.ioctx)
            if base_name in backups:
                backup_name = self._get_backup_base_name(base_id)
            vol_meta = VolumeMetadataBackup(client, backup_name)
            json_meta = vol_meta.get()
            header = jsonutils.loads(json_meta)

        return header

    def create_image(self, backup):
        """
        backup_name should be {base_id}_{backup_id}_{timestamp}
        """
        base_id = backup.source_id
        backup_id = backup.backup_id
        restore_point = backup.time_stamp
        chain_name = backup.backup_chain_name
        base_name = self._get_backup_base_name(base_id, chain_name)
        backup_name = "{0}_{1}".format(base_id, restore_point)

        with RADOSClient(self, self.ceph_backup_pool) as client:
            rbds = self.rbd.RBD().list(client.ioctx)

            if base_name in rbds:
		snap_name = self._get_new_snap_name(backup_id, restore_point)
		snap_exist = self._snap_exist(base_name, snap_name, client)
		if not snap_exist:
		    errmsg = ("Snapshot='%(snap)s' does not exist " % {'snap': snap_name})
		    LOG.info(errmsg)
		    raise exceptions.SnapshotNotFound(errmsg)
		    LOG.debug("Open rbd image %s/%s@%s", self.ceph_backup_pool, base_name, snap_name)
                src_rbd = self.rbd.Image(client.ioctx, base_name, 
                                         snapshot=snap_name, read_only=True)
            else:
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
        """Returns nova backups and cindernative backups"""
        ordered_backups = []
        with RADOSClient(self, self.ceph_backup_pool) as client:
            backups = self.rbd.RBD().list(client.ioctx)

        nova_key = CephStorage.backup_nova_name_pattern()
        backups = filter(lambda x: re.search(nova_key, x), backups)
        for backup in backups:
            ordered_backup = {}
            with RADOSClient(self, self.ceph_backup_pool) as client:
                vol_meta = VolumeMetadataBackup(client, backup)
                json_meta = vol_meta.get()

            header = None
            if json_meta:
                header = jsonutils.loads(json_meta)

            ordered_backup['backup_id'] = backup.rsplit('_', 2)[1]
            ordered_backup['source_id'] = backup.rsplit('_', 2)[0]
            ordered_backup['status'] = 'available' if header else 'error'
            ordered_backup['name'] = header['x-object-backup-name'] if header else ''

            size = (int(header['x-object-meta-length']) / 1024) / 1024 if header else 0
            if size == 0:
                size = 1
            ordered_backup['size'] = '{0}MB'.format(size)
            if size >= 1024:
                size /= 1024
                ordered_backup['size'] = '{0} GB'.format(size)
            ordered_backup['objects_count'] = 0
            ordered_backup['container'] = header['X-Object-Manifest'].split('/', 1)[0] if header else self.ceph_backup_pool
            ordered_backups.append(ordered_backup)

        cinder = self.client_manager.get_cinder()
        backups = cinder.backups.list()
        for backup in backups:
            ordered_backup = {}
            ordered_backup['backup_id'] = backup.backup_id
            ordered_backup['source_id'] = backup.volume_id
            ordered_backup['status'] = backup.status
            ordered_backup['name'] = backup.name
            ordered_backup['size'] = '{0} GB'.format(backup.size)
            ordered_backup['objects_count'] = backup.object_count
            ordered_backup['container'] = backup.container
            ordered_backups.append(ordered_backup)

        return ordered_backups

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
        split = hostname_backup_name.split('_', 1)
        cinder = self.client_manager.get_cinder()
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
            if int(remove_older_timestamp) >= int(timestamp)    \
                and backup_name == split[1]:
                LOG.debug("Deleting backup for volume %s.", backup)
                with RADOSClient(self) as client:
                    self.rbd.RBD().remove(client.ioctx, backup)

                LOG.debug("Removing metadata object for volume %s.", backup)
                with RADOSClient(self) as client:
                    VolumeMetadataBackup(client, backup).remove_if_exists()

        search_opts = {
            'name': split[1]
        }
        backups = cinder.backups.list(search_opts=search_opts)
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
        return hasattr(self.rbd, 'RBD_FEATURE_LAYERING')

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
            features |= self.rbd.RBD_FEATURE_LAYERING
        if self._supports_stripingv2:
            old_format = False
            features |= self.rbd.RBD_FEATURE_STRIPINGV2

        return (old_format, features)


class RADOSClient(object):
    """Context manager to simplify error handling for connecting to ceph."""
    def __init__(self, driver, pool=None, user=None, cluster=None, conf=None):
        self.driver = driver
        self.cluster, self.ioctx = driver._connect_to_rados(pool, user, cluster, conf)

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
        if self._exists(meta_obj):
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
