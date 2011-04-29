#!/usr/bin/env python

import os
import sys
import base64
import fnmatch
import hashlib
import yaml
import pickle
import datetime
import time
import socket
import re
import traceback

import EXIF

import gdata.photos.service
import gdata.media
import gdata.geo
import gdata.docs.data
import gdata.docs.client
import gdata.docs.service

import logging
LOG = logging.getLogger(__name__)

class GlobDirectoryWalker:
    # a forward iterator that traverses a directory tree
    #for file in GlobDirectoryWalker(".", "*.py"):
    #    print file
    def __init__(self, directory, include_pattern="*", dir_exclude_pattern=None):
        self.stack = [directory]
        self.include_pattern = include_pattern
        self.dir_exclude_pattern = dir_exclude_pattern
        self.files = []
        self.index = 0

    def is_dir_excluded(self, fullname):
        if self.dir_exclude_pattern:
            return does_match_pattern(fullname, self.dir_exclude_pattern)
        return False 
        
    def is_file_included(self, filename):
        return does_match_pattern(filename, self.include_pattern)
    
    def __getitem__(self, index):
        while 1:
            try:
                file = self.files[self.index]
                self.index = self.index + 1
            except IndexError:
                # pop next directory from stack
                self.directory = self.stack.pop()
                self.files = map(fs_unic, os.listdir(self.directory))
                self.index = 0
            else:
                # got a filename
                fullname = os.path.join(self.directory, file)
                if os.path.isdir(fullname) and not os.path.islink(fullname) and not self.is_dir_excluded(fullname):
                    self.stack.append(fullname)
                if self.is_file_included(fullname):
                    return fullname

def request_access(gd_client, domain="default"):
    # Installed applications do not have a pre-registration and so follow
    # directions for unregistered applications
    gd_client.SetOAuthInputParameters(gdata.auth.OAuthSignatureMethod.HMAC_SHA1,
                                      consumer_key='anonymous',
                                      consumer_secret='anonymous')
    display_name = 'picasa-folder-sync'
    fetch_params = {'xoauth_displayname':display_name}
    # First and third if statements taken from
    # gdata.service.GDataService.FetchOAuthRequestToken.
    # Need to do this detection/conversion here so we can add the 'email' API
    scopes = list(gdata.service.lookup_scopes('lh2'))

    try:
        request_token = gd_client.FetchOAuthRequestToken(scopes=scopes,
                                                         extra_parameters=fetch_params)
    except gdata.service.FetchingOAuthRequestTokenFailed, err:
        print err[0]['body'].strip() + '; Request token retrieval failed!'
        return False

    auth_params = {'hd': domain}
    auth_url = gd_client.GenerateOAuthAuthorizationURL(request_token=request_token,
                                                       extra_params=auth_params)
    message = 'Please log in and/or grant access via your browser at ' +\
            auth_url + ' then hit enter.'
    raw_input(message)

    # This upgrades the token, and if successful, sets the access token
    try:
        gd_client.UpgradeToOAuthAccessToken(request_token)
    except gdata.service.TokenUpgradeFailed:
        print 'Token upgrade failed! Could not get OAuth access token.'
        return False
    else:
        return True

def mustbelist(obj):
    return obj if isinstance(obj, (list, tuple)) else [obj]

def unic(obj, encoding='utf8'):
    return obj.decode(encoding) if isinstance(obj, str) else obj

def fs_unic(obj, encoding=sys.getfilesystemencoding()):
    return unic(obj, encoding=encoding)

def does_match_pattern(name, pattern):
    name = name.upper()
    return any(fnmatch.fnmatch(name, p.upper()) for p in mustbelist(pattern))

def get_photo_title(filename, album_path):
    if filename.find(album_path) == 0:
        filename = filename[len(album_path)+1:]
        filename = filename.replace('/', '_')
        filename = filename.replace('\\', '_')
        
    return fs_unic(filename)

def md5_for_file(f, block_size=2**20):
    f.seek(0)
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def md5_for_string(s):
    md5 = hashlib.md5()
    if isinstance(s, unicode):
        s = s.encode('utf-8')
    md5.update(s)
    return md5.hexdigest()

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

def get_content_type_from_extension(extension):
    extension_to_content_type = {'jpg': 'image/jpeg',
                                 'jpeg': 'image/jpeg',
                                 'bmp': 'image/bmp',
                                 'gif': 'image/gif',
                                 'png': 'image/png',
                                 'mov': 'video/mpeg',
                                 'mpg': 'video/mpeg'}
    return extension_to_content_type.get(extension)
    
class Album(object):
    def __init__(self, directory, title, include_files, exclude_dirs):
        self.directory = directory
        self.title = title
        self.include_files = include_files
        self.exclude_dirs = exclude_dirs
        self.picasa_sync_config = None
        self.picasa_sync_config_filename = os.path.join(directory, '.picasa-sync')
        self.synced_photos_by_id_map = {}
        self.synced_album_gphoto_id = ""

        # If the directory has been synchronized before it will contain a .picasa-sync file with the state from the last sync.
        if os.path.exists(self.picasa_sync_config_filename):
            with open(self.picasa_sync_config_filename) as f:
                print "opening .picasa-sync"
                picasa_sync_config = yaml.load(f)
                self.synced_photos_by_id_map = picasa_sync_config['photos_by_id_map']
                
                # Compensate for old bug where filenames were written down in str format instead of unicode
                for gphoto_id, (filename, checksum) in self.synced_photos_by_id_map.iteritems():
                    if isinstance(filename, str):
                        print "Changing filename encoding for " + filename
                        self.synced_photos_by_id_map[gphoto_id] = [fs_unic(filename), checksum]
                        
                self.synced_album_gphoto_id = picasa_sync_config['album_gphoto_id']
                print "GPhoto ID: %s" % self.synced_album_gphoto_id

        self.synced_photos_by_filename_map = dict([(filename, gphoto_id) for gphoto_id, (filename, checksum) in self.synced_photos_by_id_map.iteritems()])
        self.album_datetime = datetime.datetime.now()
        self.file_data_list = []
        self.online_album = None
            
    def _load_file_data_list(self):
        self.album_datetime = datetime.datetime.max
        file_data_list = []
        movies = set()
        
        filenames = [filename for filename in GlobDirectoryWalker(self.directory, self.include_files, self.exclude_dirs)]
        for filename in filenames:
            basename, extension = os.path.splitext(filename)
            
            with open(filename) as file:
                tags = EXIF.process_file(file, stop_tag='Image DateTime', details=False)
                if 'Image DateTime' in tags:
                    dt = datetime.datetime.strptime(str(tags['Image DateTime']), "%Y:%m:%d %H:%M:%S")
                else:
                    dt = modification_date(filename)
                
                # Set album time to the time of the oldest photo in the album
                if dt < self.album_datetime:
                    self.album_datetime = dt

                print "%s: %s" % (filename, dt)

                file_size = os.path.getsize(filename)
                if file_size < 100*(2**20):    
                    checksum = md5_for_file(file)
                else:
                    checksum = md5_for_string(filename+unicode(file_size))
                file_data = {'filename': filename, 'datetime': dt, 'checksum': checksum}
                file_data_list.append(file_data)
                
                # Maintain a set of all movies to filter out thumbnail images below.
                if extension.lower() in ('.mov', '.mpg', '.mpeg'):
                    movies.add(basename)
        
        # Assume that thUmbnail images have the same filename as the movie, but an image extension.
        self.file_data_list = []
        for file_data in file_data_list:
            basename, extension = os.path.splitext(file_data['filename'])
            if extension.lower() not in ('.bmp', '.jpeg', '.jpg', '.gif', '.png') or basename not in movies:
                self.file_data_list.append(file_data)
            else:
                print "Image assumed to be a movie thumbnail: " + file_data['filename'] + " - skipping!"

        # Make sure the list is sorted on datetime
        self.file_data_list.sort(lambda x, y: cmp(x['datetime'], y['datetime']))
        
        # If no files then we set the album time to the current time.
        if len(self.file_data_list) == 0:
            self.album_datetime = datetime.datetime.now()
    
    def _save_picasa_sync_config(self):
        with open(self.picasa_sync_config_filename, 'w') as f:
            yaml.dump({"photos_by_id_map": self.synced_photos_by_id_map, "album_gphoto_id": self.synced_album_gphoto_id}, f)
        
    def _create_or_update_online_album(self, ps_client):
        if self.online_album:
            assert self.online_album.gphoto_id.text == self.synced_album_gphoto_id        
            # LOG.debug('online.title=%r ? title=%r', self.online_album.title.text, self.title)
            if unic(self.online_album.title.text) == self.title and self.online_album.timestamp.datetime() == self.album_datetime:
                print "Album %s already exists and is up to date" % self.title
            else:
                old_online_album_title = unic(self.online_album.title.text)
                self.online_album.title.text = self.title
                self.online_album.timestamp.text = str(int(time.mktime(self.album_datetime.timetuple())*1000))
                ps_client.Put(self.online_album, self.online_album.GetEditLink().href, converter=gdata.photos.AlbumEntryFromString)
                print u"Existing album %s updated (title: %s, timestamp: %s)" % (old_online_album_title, self.title, self.album_datetime)
                self._save_picasa_sync_config()
        else:
            print u"Creating new album %s" % self.title
            timestamp = str(int(time.mktime(self.album_datetime.timetuple())*1000))
            self.online_album = ps_client.InsertAlbum(title=self.title, summary=None, location=None, access='private', commenting_enabled='true', timestamp=timestamp)
            self.synced_album_gphoto_id = self.online_album.gphoto_id.text
            self._save_picasa_sync_config()
               
    def _create_or_update_online_files(self, ps_client):
        print "Getting list of photos/videos for %s" % self.title
        existing_photos = ps_client.GetFeed('/data/feed/api/user/default/albumid/%s?kind=photo' % (self.synced_album_gphoto_id))
        id_existing_photos_map = dict([(photo.gphoto_id.text, photo) for photo in existing_photos.entry])
        updated_online_photos = set()
        
        # Build maps of files in directory
        checksum_to_filename_map = {}
        filename_to_checksum_map = {}
        duplicate_checksums = set()
        for file_data in self.file_data_list:
            file_checksum = file_data['checksum']
            if file_checksum in checksum_to_filename_map:
                duplicate_checksums.add(file_checksum)

            checksum_to_filename_map[file_checksum] = file_data['filename']
            filename_to_checksum_map[file_data['filename']] = file_checksum

        # Use the filename to checksum map to detect renamed files. We skip files
        # with checksums that exist more than once in the directory, since we are
        # then unable to distinguish the files.
        rename_to_from_map = {}
        for gphoto_id, (filename, checksum) in self.synced_photos_by_id_map.iteritems():
            if checksum in checksum_to_filename_map and not checksum in duplicate_checksums:
                if filename != checksum_to_filename_map[checksum]:
                    if self.synced_photos_by_filename_map[filename] in id_existing_photos_map:
                        rename_to_from_map[checksum_to_filename_map[checksum]] = filename

        # Now update or create the online version
        for file_data in self.file_data_list:
            filename = file_data['filename']
            file_checksum = filename_to_checksum_map[filename]
            
            # Check if the file needs to be renamed.
            if filename in rename_to_from_map:
                rename_to_title = get_photo_title(filename, self.directory)
                rename_from_filename = rename_to_from_map[filename]
                gphoto_id = self.synced_photos_by_filename_map[rename_from_filename]
                photo = id_existing_photos_map[gphoto_id]
                photo.title.text = rename_to_title
                print u"Updating photo title from %s to %s" % (get_photo_title(rename_from_filename, self.directory), rename_to_title)
                photo = ps_client.UpdatePhotoMetadata(photo)
                
                # Update local state
                updated_online_photos.add(photo.gphoto_id.text)
                self.synced_photos_by_id_map[photo.gphoto_id.text] = [filename, file_checksum]
                self.synced_photos_by_filename_map[filename] = photo.gphoto_id.text
                self._save_picasa_sync_config()
                
            # If the local file does not exist online, we need to add it.
            elif not filename in self.synced_photos_by_filename_map or not self.synced_photos_by_filename_map[filename] in id_existing_photos_map:
                photo_title = get_photo_title(filename, self.directory)
                root, extension = os.path.splitext(filename)
                extension = extension[1:].lower()
                content_type = get_content_type_from_extension(extension)
                if extension and content_type:
                    file_size = os.path.getsize(filename)
                    if file_size < 100*(2**20):
                        print "Inserting new photo/video for %s" % filename
                        photo = ps_client.InsertPhotoSimple(self.online_album, photo_title, "", filename, content_type)
                        
                        # Update local state
                        updated_online_photos.add(photo.gphoto_id.text)
                        self.synced_photos_by_id_map[photo.gphoto_id.text] = [filename, file_checksum]
                        self.synced_photos_by_filename_map[filename] = photo.gphoto_id.text                    
                        self._save_picasa_sync_config()            
                    else:
                        print "Skipping too large (%d MB) photo/video: %s" % ((int)(file_size/1024.0/1024.0), filename)

            # The image already existed online - check if we need to update content.
            if filename in self.synced_photos_by_filename_map and self.synced_photos_by_filename_map[filename] in id_existing_photos_map:
                # Get photo ID and checksum from previous sync.
                gphoto_id = self.synced_photos_by_filename_map[filename]
                existing_checksum = self.synced_photos_by_id_map[gphoto_id][1]
                photo = id_existing_photos_map[gphoto_id]
        
                # Update picture data if checksums differ.
                if file_checksum != existing_checksum:
                    root, extension = os.path.splitext(filename)
                    extension = extension[1:].lower()
                    content_type = get_content_type_from_extension(extension)
                    if extension and content_type:
                        file_size = os.path.getsize(filename)
                        if file_size < 100*(2**20):
                            if 'image' in content_type:
                                print "Updating photo blob for %s" % filename
                                ps_client.UpdatePhotoBlob(photo, filename, content_type)                           
                            else:
                                print "Inserting new video for %s (not able to update videos)" % filename
                                photo_title = get_photo_title(filename, self.directory)
                                photo = ps_client.InsertPhotoSimple(self.online_album, photo_title, "", filename, content_type)

                            # Update local state
                            updated_online_photos.add(photo.gphoto_id.text)
                            self.synced_photos_by_id_map[photo.gphoto_id.text] = [filename, file_checksum]
                            self.synced_photos_by_filename_map[filename] = photo.gphoto_id.text                        
                            self._save_picasa_sync_config()
                        else:
                            print "Not able to update too large (%d MB) photo/video: %s" % ((int)(file_size/1024.0/1024.0), filename)
                else:
                    updated_online_photos.add(photo.gphoto_id.text)
                    print "Photo/video %s already up to date" % filename

        # Now delete any photos that no longer exists.
        for gphoto_id, photo in id_existing_photos_map.iteritems():
            if not gphoto_id in updated_online_photos:
                print "Deleting photo/video %s" % photo.title.text
                ps_client.Delete(photo)
        
        # Update synced photos by ID to only reflect photos that have been updated during this run.
        filenames_check = set()
        new_synced_photos_by_id_map = {}
        for gphoto_id, (filename, checksum) in self.synced_photos_by_id_map.iteritems():            
            if gphoto_id in updated_online_photos:
                assert not filename in filenames_check
                filenames_check.add(filename)
                new_synced_photos_by_id_map[gphoto_id] = self.synced_photos_by_id_map[gphoto_id]
        self.synced_photos_by_id_map = new_synced_photos_by_id_map
        self._save_picasa_sync_config()
        
    def update_online_album(self, ps_client):
        if len(self.file_data_list) == 0:
            self._load_file_data_list()
        
        if len(self.file_data_list) > 0:
            self._create_or_update_online_album(ps_client)
            if self.online_album:
                self._create_or_update_online_files(ps_client)
                return True

        return False
        
def generate_default_config_file(filename):
    f = open(filename, "w")
    yaml.dump({
        "account": ["account@gmail.com", None], # Gmail Account, Token 
        "photo_dir": os.path.expanduser("~/Pictures"), # The directory to synchronize with the picasaweb account
        "include_files": ["*.jpg", "*.jpeg", "*.bmp", "*.gif", "*.png", "*.mov", "*.mpg"], # Files of these types will be considered for upload.
        "exclude_dirs": [".DS_Store"], # Directory names in this list will be exluded
        "delete_online_albums_not_local": False, # When this is true any existing online album that does not exist locally will be deleted
        "never_delete_online_albums": ["Camera Roll"], # Online album names in this list will never be deleted.
        "update_local_albums_already_online": False}, f) # This decides whether albums that have been uploaded previously will be updated.
    
def main(argv):
    if len(argv) == 1:
        config_filename = argv[0]
    else:
        config_filename = os.path.expanduser("~/.picasa-directory-sync-conf")

    if not os.path.exists(config_filename):
        generate_default_config_file(config_filename)
    
    with open(config_filename, "r") as config_file:
        config = yaml.load(config_file)
        user_account = config['account'][0]
        token = config['account'][1]
        photo_dir = config['photo_dir']
        include_files = config['include_files']
        exclude_dirs = config['exclude_dirs']
        delete_online_albums_not_local = config['delete_online_albums_not_local']
        never_delete_online_albums = config['never_delete_online_albums']
        update_local_albums_already_online = config['update_local_albums_already_online']
    
    gdata.photos.service.SUPPORTED_UPLOAD_TYPES = ('bmp', 'jpeg', 'jpg', 'gif', 'png', 'mov', 'mpg', 'mpeg')
    
    gd_client = gdata.photos.service.PhotosService()
    gd_client.ssl = False
    gd_client.email = user_account
    if token:
        gd_client.SetOAuthToken(token)
    else:
        if request_access(gd_client):
            config['account'][1] = gd_client.current_token
            with open(config_filename, "w") as config_file:
                yaml.dump(config, config_file)
        else:
            print 'Failed to request access'
            return
                
    try:
        print "Getting online albums"
        online_albums = gd_client.GetUserFeed()
        id_to_online_album_map = dict([(album.gphoto_id.text, album) for album in online_albums.entry])
        
        print "Getting local albums"
        local_albums = map(fs_unic, [local_album_title for local_album_title in os.listdir(photo_dir)])
        # LOG.debug('local_albums: %r', local_albums)
        local_albums.sort(key=lambda s: s.lower(), reverse=True)
        expr = re.compile("\[\d{4,4}-\d{2,2}-\d{2,2}\] (.+)")
        
        for local_album_title in local_albums:
            directory = os.path.join(photo_dir, local_album_title)
            if not os.path.isdir(directory) or os.path.islink(directory) or does_match_pattern(local_album_title, exclude_dirs):
                continue
            
            # Check if the album is prefixed with date.
            m = expr.match(local_album_title)
            if m != None:
                local_album_title = m.group(1)              
                    
            album = Album(directory, local_album_title, include_files, exclude_dirs)
            
            # Set the online album if it exists.
            if album.synced_album_gphoto_id in id_to_online_album_map:
                album.online_album = id_to_online_album_map[album.synced_album_gphoto_id]
            
            retry_count = 0
            while True:
                try:
                    retry_count += 1                
                                     
                    # Update the online album from the local directory.
                    if not album.online_album or update_local_albums_already_online:
                        album.update_online_album(gd_client)

                    # Remove the album from the existing online albums map. Then we
                    # can delete all remaining albums when sync is completed.
                    if album.synced_album_gphoto_id in id_to_online_album_map:
                        del id_to_online_album_map[album.synced_album_gphoto_id]
                    
                except Exception, e:              
                    if "Token invalid" in str(e):
                        raise
                    
                    if retry_count <= 10:
                        traceback.print_exc()
                        print "Exception occurred (%s) - sleeping for 2 minuttes before retrying." % str(e)
                        time.sleep(120) # Sleep for 2 mins.
                    else:
                        traceback.print_exc()
                        print "Maximum album retry count exceeded - aborting."
                        return              
                else:
                    # Album updated - break from inner loop
                    break 
                                       
        # Delete albums online that no longer exist locally, if enabled.
        while delete_online_albums_not_local:
            try:                                       
                for album in id_to_online_album_map.values():
                    if not album.title.text in never_delete_online_albums:
                        print "Deleting album %s" % album.title.text
                        gd_client.Delete(album)
                        del id_to_online_album_map[album.gphoto_id.text]
            except Exception, e:         
                print "Exception occurred (%s) - sleeping for 2 minuttes before retrying." % str(e)   
                time.sleep(120) # Sleep for 2 mins.
            else:
                # Albums delete break from loop.
                break
                
        # Delete empty online albums
        online_albums = gd_client.GetUserFeed()
        online_albums = [album for album in online_albums.entry]
        while True:
            try:
                i = len(online_albums)
                while i != 0:
                    i -= 1
                    online_album = online_albums[i]
                    if int(online_album.numphotos.text) == 0 and not online_album.title.text in never_delete_online_albums:
                        print "Deleting empty album: %s" % online_album.title.text
                        gd_client.Delete(online_album)
                        del online_albums[i]
            except Exception, e:
                print "Exception occurred (%s) - sleeping for 2 minuttes before retrying." % str(e)       
                time.sleep(120) # Sleep for 2 mins.
            else:
                # Albums delete break from loop.
                break
            
        print "DONE!"   
    except gdata.photos.service.GooglePhotosException, e:
        if "Token invalid" in str(e):
            print "Auth token was invalid - deleted."
            os.remove(token_filename)
        else:
            raise
            
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv[1:])
