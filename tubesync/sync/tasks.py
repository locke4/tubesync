'''
    Start, stop and manage scheduled tasks. These are generally triggered by Django
    signals (see signals.py).
'''


import os
import json
import math
import uuid
from io import BytesIO
from hashlib import sha1
from datetime import timedelta, datetime
from shutil import copyfile
from PIL import Image
from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.utils import timezone
from django.db.utils import IntegrityError
from django.utils.translation import gettext_lazy as _
from background_task import background
from background_task.models import Task, CompletedTask
from common.logger import log
from common.errors import NoMediaException, DownloadFailedException
from common.utils import json_serial
from .models import Source, Media, MediaServer
from .utils import (get_remote_image, resize_image_to_height, delete_file,
                    write_text_file)
import time
from celery import shared_task

@shared_task
def create_task():
    log.info("debug task")
    return True

@shared_task
def pre_update_source(source_id):
    try:
        source = Source.objects.get(pk=source_id)
        log.info(f'Updating source {source}')
        delete_task_by_source('sync.tasks.index_source_task', source_id)
        verbose_name = _('Index media from source "{}"')
        index_source_task(
            str(source_id),
            repeat=source.index_schedule,
            queue=str(source_id),
            priority=5,
            verbose_name=verbose_name.format(source.name),
            remove_existing_tasks=True
        )

    except Source.DoesNotExist:
        # Task triggered but the Source has been deleted, delete the task
        log.info(f'Source does not exist with id {source_id}')
        return

@shared_task
def post_update_source(source_id, name, created, index_schedule):
    log.info(f'running post-update task {source_id} with {created}')
    source = Source.objects.get(pk=source_id)
    if created:
        log.info(f'check source directory exists for {source_id}')
        verbose_name = _('Check download directory exists for source "{}"')
        check_source_directory_exists(
            str(source_id),
            priority=0,
            verbose_name=verbose_name.format(name)
        )
        if index_schedule > 0:
            delete_task_by_source('sync.tasks.index_source_task', source_id)
            log.info(f'Scheduling media indexing for source: {name}')
            verbose_name = _('Index media from source "{}"')
            index_source_task(
                str(source_id),
                repeat=index_schedule,
                queue=str(source_id),
                priority=5,
                verbose_name=verbose_name.format(name),
                remove_existing_tasks=True
            )
    for media in Media.objects.filter(source=source):
        media.save()

@shared_task
def post_media_save(media_id):
    #log.info('post_media_save')
    
    media = Media.objects.get(pk=media_id)
    log.info(f'post_media_save for {media.pk}')
    
    cap_changed = False
    can_download_changed = False
    # Reset the skip flag if the download cap has changed if the media has not
    # already been downloaded
    if not media.downloaded:
        max_cap_age = media.source.download_cap_date
        filter_text = media.source.filter_text
        published = media.published 

        if media.skip:
            #currently marked to be skipped, check if skip conditions still apply
            if not published:
                log.debug(f'Media: {media.source} / {media} has no published date '
                        f'set but is already marked to be skipped')
            else:            
                if max_cap_age and filter_text:
                    if (published > max_cap_age) and (media.source.is_regex_match(media.title)):
                        # Media was published after the cap date and matches the filter text, but is set to be skipped
                        print('Has a valid publishing date and matches filter, marking unskipped')
                        media.skip = False
                        cap_changed = True
                    else:
                        print('does not have a valid publishing date or filter string, already marked skipped')
                        log.info(f'Media: {media.source} / {media} has no published date '
                                f'set but is already marked to be skipped')
                elif max_cap_age:
                    if published > max_cap_age:
                        # Media was published after the cap date but is set to be skipped
                        log.info(f'Media: {media.source} / {media} has a valid '
                                f'publishing date, marking to be unskipped')
                        media.skip = False
                        cap_changed = True
                elif filter_text:
                    if media.source.is_regex_match(media.title):
                        # Media matches the filter text but is set to be skipped
                        log.info(f'Media: {media.source} / {media} matches the filter text, marking to be unskipped')
                        media.skip = False
                        cap_changed = True
        else:
            if not published:
                log.info(f'Media: {media.source} / {media} has no published date, marking to be skipped')
                media.skip = True
                cap_changed = True
            else:
                if max_cap_age:
                    if published <= max_cap_age:            
                        log.info(f'Media: {media.source} / {media} is too old for '
                                f'the download cap date, marking to be skipped')
                        media.skip = True
                        cap_changed = True
                if filter_text:
                    if not media.source.is_regex_match(media.title):
                        #media doesn't match the filter text but is not marked to be skipped
                        log.info(f'Media: {media.source} / {media} does not match the filter text')
                        media.skip = True
                        cap_changed = True
      
    # Recalculate the "can_download" flag, this may
    # need to change if the source specifications have been changed
    if media.metadata:
        if media.get_format_str():
            if not media.can_download:
                media.can_download = True
                can_download_changed = True
        else:
            if media.can_download:
                media.can_download = False
                can_download_changed = True
    # Save the instance if any changes were required
    if cap_changed or can_download_changed:
        #post_save.disconnect(media_post_save, sender=Media)
        media.save()
        #post_save.connect(media_post_save, sender=Media)
    # If the media is missing metadata schedule it to be downloaded
    if not media.metadata:

        log.info(f'Scheduling task to download metadata for: {media.url}')
        pre_download_media_metadata(media.pk)
        """verbose_name = _('Downloading metadata for "{}"')
        download_media_metadata(
            str(media.pk),
            priority=5,
            verbose_name=verbose_name.format(media.pk),
            remove_existing_tasks=True
        )"""
    # If the media is missing a thumbnail schedule it to be downloaded
    if not media.thumb_file_exists:
        media.thumb = None
    if not media.thumb:
        thumbnail_url = media.thumbnail
        if thumbnail_url:
            log.info(f'Scheduling task to download thumbnail for: {media.name} '
                     f'from: {thumbnail_url}')
            pre_download_media_thumbnail(media.pk, thumbnail_url)
            """verbose_name = _('Downloading thumbnail for "{}"')
            download_media_thumbnail(
                str(media.pk),
                thumbnail_url,
                queue=str(media.source.pk),
                priority=10,
                verbose_name=verbose_name.format(media.name),
                remove_existing_tasks=True
            )"""
    # If the media has not yet been downloaded schedule it to be downloaded
    if not media.media_file_exists:
        media.downloaded = False
        media.media_file = None
    if (not media.downloaded and media.can_download and not media.skip
        and media.source.download_media):
        delete_task_by_media('sync.tasks.download_media', (str(media.pk),))
        verbose_name = _('Downloading media for "{}"')
        download_media(
            str(media.pk),
            queue=str(media.source.pk),
            priority=15,
            verbose_name=verbose_name.format(media.name),
            remove_existing_tasks=True
        )
    return

@shared_task
def pre_download_media_metadata(media_id):
    verbose_name = _('Downloading metadata for "{}"')
    media = Media.objects.get(pk=media_id)
    download_media_metadata(
        str(media.pk),
        priority=5,
        verbose_name=verbose_name.format(media.pk),
        remove_existing_tasks=True
    )
    return
@shared_task
def pre_download_media_thumbnail(media_id, thumbnail_url):
    verbose_name = _('Downloading thumbnail for "{}"')
    media = Media.objects.get(pk=media_id)    
    download_media_thumbnail(
        str(media.pk),
        thumbnail_url,
        queue=str(media.source.pk),
        priority=10,
        verbose_name=verbose_name.format(media.name),
        remove_existing_tasks=True
    )
    return

@shared_task
def pre_source_delete(source_id):
    log.info(f'running pre-delete task for source {source_id}')
    source = Source.objects.get(pk=source_id)
    for media in Media.objects.filter(source=source):
        log.info(f'Deleting media for source: {source.name} item: {media.name}')
        media.delete()
    return

def get_hash(task_name, pk):
    '''
        Create a background_task compatible hash for a Task or CompletedTask.
    '''
    task_params = json.dumps(((str(pk),), {}), sort_keys=True)
    return sha1(f'{task_name}{task_params}'.encode('utf-8')).hexdigest()


def map_task_to_instance(task):
    '''
        Reverse-maps a scheduled backgrond task to an instance. Requires the task name
        to be a known task function and the first argument to be a UUID. This is used
        because UUID's are incompatible with background_task's "creator" feature.
    '''
    TASK_MAP = {
        'sync.tasks.index_source_task': Source,
        'sync.tasks.check_source_directory_exists': Source,
        'sync.tasks.download_media_thumbnail': Media,
        'sync.tasks.download_media': Media,
    }
    MODEL_URL_MAP = {
        Source: 'sync:source',
        Media: 'sync:media-item',
    }
    # Unpack
    task_func, task_args_str = task.task_name, task.task_params
    model = TASK_MAP.get(task_func, None)
    if not model:
        return None, None
    url = MODEL_URL_MAP.get(model, None)
    if not url:
        return None, None
    try:
        task_args = json.loads(task_args_str)
    except (TypeError, ValueError, AttributeError):
        return None, None
    if len(task_args) != 2:
        return None, None
    args, kwargs = task_args
    if len(args) == 0:
        return None, None
    instance_uuid_str = args[0]
    try:
        instance_uuid = uuid.UUID(instance_uuid_str)
    except (TypeError, ValueError, AttributeError):
        return None, None
    try:
        instance = model.objects.get(pk=instance_uuid)
        return instance, url
    except model.DoesNotExist:
        return None, None


def get_error_message(task):
    '''
        Extract an error message from a failed task. This is the last line of the
        last_error field with the method name removed.
    '''
    if not task.has_error():
        return ''
    stacktrace_lines = task.last_error.strip().split('\n')
    if len(stacktrace_lines) == 0:
        return ''
    error_message = stacktrace_lines[-1].strip()
    if ':' not in error_message:
        return ''
    return error_message.split(':', 1)[1].strip()


def get_source_completed_tasks(source_id, only_errors=False):
    '''
        Returns a queryset of CompletedTask objects for a source by source ID.
    '''
    q = {'queue': source_id}
    if only_errors:
        q['failed_at__isnull'] = False
    return CompletedTask.objects.filter(**q).order_by('-failed_at')


def get_media_download_task(media_id):
    try:
        return Task.objects.get_task('sync.tasks.download_media',
                                     args=(str(media_id),))[0]
    except IndexError:
        return False


def delete_task_by_source(task_name, source_id):
    return Task.objects.filter(task_name=task_name, queue=str(source_id)).delete()


def delete_task_by_media(task_name, args):
    return Task.objects.drop_task(task_name, args=args)


def cleanup_completed_tasks():
    days_to_keep = getattr(settings, 'COMPLETED_TASKS_DAYS_TO_KEEP', 30)
    delta = timezone.now() - timedelta(days=days_to_keep)
    log.info(f'Deleting completed tasks older than {days_to_keep} days '
             f'(run_at before {delta})')
    CompletedTask.objects.filter(run_at__lt=delta).delete()


def cleanup_old_media():
    for source in Source.objects.filter(delete_old_media=True, days_to_keep__gt=0):
        delta = timezone.now() - timedelta(days=source.days_to_keep)
        for media in source.media_source.filter(downloaded=True, download_date__lt=delta):
            log.info(f'Deleting expired media: {source} / {media} '
                     f'(now older than {source.days_to_keep} days / '
                     f'download_date before {delta})')
            # .delete() also triggers a pre_delete signal that removes the files
            media.delete()


def cleanup_removed_media(source, videos):
    media_objects = Media.objects.filter(source=source, downloaded=True)
    for item in media_objects:
        matching_source_item = [video['id'] for video in videos if video['id'] == item.key]
        if not matching_source_item:
            log.info(f'{item.title} is no longer in source, removing')
            item.delete()


@background(schedule=0)
def index_source_task(source_id):
    '''
        Indexes media available from a Source object.
    '''
    try:
        source = Source.objects.get(pk=source_id)
    except Source.DoesNotExist:
        # Task triggered but the Source has been deleted, delete the task
        return
    # Reset any errors
    source.has_failed = False
    source.save()
    # Index the source
    videos = source.index_media()
    if not videos:
        raise NoMediaException(f'Source "{source}" (ID: {source_id}) returned no '
                               f'media to index, is the source key valid? Check the '
                               f'source configuration is correct and that the source '
                               f'is reachable')
    # Got some media, update the last crawl timestamp
    source.last_crawl = timezone.now()
    source.save()
    log.info(f'Found {len(videos)} media items for source: {source}')
    for video in videos:
        # Create or update each video as a Media object
        key = video.get(source.key_field, None)
        if not key:
            # Video has no unique key (ID), it can't be indexed
            continue
        try:
            media = Media.objects.get(key=key, source=source)
        except Media.DoesNotExist:
            media = Media(key=key)
        media.source = source
        try:
            media.save()
            log.info(f'Indexed media: {source} / {media}')
        except IntegrityError as e:
            log.error(f'Index media failed: {source} / {media} with "{e}"')
    # Tack on a cleanup of old completed tasks
    cleanup_completed_tasks()
    # Tack on a cleanup of old media
    cleanup_old_media()
    if source.delete_removed_media:
        log.info(f'Cleaning up media no longer in source {source}')
        cleanup_removed_media(source, videos)


@background(schedule=0)
def check_source_directory_exists(source_id):
    '''
        Checks the output directory for a source exists and is writable, if it does
        not attempt to create it. This is a task so if there are permission errors
        they are logged as failed tasks.
    '''
    try:
        source = Source.objects.get(pk=source_id)
    except Source.DoesNotExist:
        # Task triggered but the Source has been deleted, delete the task
        return
    # Check the source output directory exists
    if not source.directory_exists():
        # Try and create it
        log.info(f'Creating directory: {source.directory_path}')
        source.make_directory()


@background(schedule=0)
def download_media_metadata(media_id):
    '''
        Downloads the metadata for a media item.
    '''
    try:
        media = Media.objects.get(pk=media_id)
    except Media.DoesNotExist:
        # Task triggered but the media no longer exists, do nothing
        log.error(f'Task download_media_metadata(pk={media_id}) called but no '
                  f'media exists with ID: {media_id}')
        return
    if media.manual_skip:
        log.info(f'Task for ID: {media_id} skipped, due to task being manually skipped.')
        return
    source = media.source
    metadata = media.index_metadata()
    media.metadata = json.dumps(metadata, default=json_serial)
    upload_date = media.upload_date
    # Media must have a valid upload date
    if upload_date:
        media.published = timezone.make_aware(upload_date)
    else:
        log.error(f'Media has no upload date, skipping: {source} / {media}')
        media.skip = True
    # If the source has a download cap date check the upload date is allowed
    max_cap_age = source.download_cap_date
    if media.published and max_cap_age:
        if media.published < max_cap_age:
            # Media was published after the cap date, skip it
            log.warn(f'Media: {source} / {media} is older than cap age '
                     f'{max_cap_age}, skipping')
            media.skip = True
    # If the source has a search filter, check the video title matches the filter
    if source.filter_text and not source.is_regex_match(media.title):
        # Filter text not found in the media title. Accepts regex string, blank search filter results in this returning false
        log.warn(f'Media: {source} / {media} does not match {source.filter_text}, skipping')
        media.skip = True
    # If the source has a cut-off check the upload date is within the allowed delta
    if source.delete_old_media and source.days_to_keep > 0:
        if not isinstance(media.published, datetime):
            # Media has no known published date or incomplete metadata
            log.warn(f'Media: {source} / {media} has no published date, skipping')
            media.skip = True
        else:
            delta = timezone.now() - timedelta(days=source.days_to_keep)
            if media.published < delta:
                # Media was published after the cutoff date, skip it
                log.warn(f'Media: {source} / {media} is older than '
                         f'{source.days_to_keep} days, skipping')
                media.skip = True
    # Check we can download the media item
    if not media.skip:
        if media.get_format_str():
            media.can_download = True
        else:
            media.can_download = False
    # Save the media
    media.save()
    log.info(f'Saved {len(media.metadata)} bytes of metadata for: '
             f'{source} / {media_id}')


@background(schedule=0)
def download_media_thumbnail(media_id, url):
    '''
        Downloads an image from a URL and save it as a local thumbnail attached to a
        Media instance.
    '''
    try:
        media = Media.objects.get(pk=media_id)
    except Media.DoesNotExist:
        # Task triggered but the media no longer exists, do nothing
        return
    width = getattr(settings, 'MEDIA_THUMBNAIL_WIDTH', 430)
    height = getattr(settings, 'MEDIA_THUMBNAIL_HEIGHT', 240)
    i = get_remote_image(url)
    log.info(f'Resizing {i.width}x{i.height} thumbnail to '
             f'{width}x{height}: {url}')
    i = resize_image_to_height(i, width, height)
    image_file = BytesIO()
    i.save(image_file, 'JPEG', quality=85, optimize=True, progressive=True)
    image_file.seek(0)
    media.thumb.save(
        'thumb',
        SimpleUploadedFile(
            'thumb',
            image_file.read(),
            'image/jpeg',
        ),
        save=True
    )
    log.info(f'Saved thumbnail for: {media} from: {url}')
    return True


@background(schedule=0)
def download_media(media_id):
    '''
        Downloads the media to disk and attaches it to the Media instance.
    '''
    try:
        media = Media.objects.get(pk=media_id)
    except Media.DoesNotExist:
        # Task triggered but the media no longer exists, do nothing
        return
    if media.skip:
        # Media was toggled to be skipped after the task was scheduled
        log.warn(f'Download task triggered for  media: {media} (UUID: {media.pk}) but '
                 f'it is now marked to be skipped, not downloading')
        return
    if media.downloaded and media.media_file:
        # Media has been marked as downloaded before the download_media task was fired,
        # skip it
        log.warn(f'Download task triggered for media: {media} (UUID: {media.pk}) but '
                 f'it has already been marked as downloaded, not downloading again')
        return
    if not media.source.download_media:
        log.warn(f'Download task triggered for media: {media} (UUID: {media.pk}) but '
                 f'the source {media.source} has since been marked to not download, '
                 f'not downloading')
        return
    max_cap_age = media.source.download_cap_date
    published = media.published
    if max_cap_age and published:
        if published <= max_cap_age:
            log.warn(f'Download task triggered media: {media} (UUID: {media.pk}) but '
                     f'the source has a download cap and the media is now too old, '
                     f'not downloading')
            return
    filepath = media.filepath
    log.info(f'Downloading media: {media} (UUID: {media.pk}) to: "{filepath}"')
    format_str, container = media.download_media()
    if os.path.exists(filepath):
        # Media has been downloaded successfully
        log.info(f'Successfully downloaded media: {media} (UUID: {media.pk}) to: '
                 f'"{filepath}"')
        # Link the media file to the object and update info about the download
        media.media_file.name = str(media.source.type_directory_path / media.filename)
        media.downloaded = True
        media.download_date = timezone.now()
        media.downloaded_filesize = os.path.getsize(filepath)
        media.downloaded_container = container
        if '+' in format_str:
            # Seperate audio and video streams
            vformat_code, aformat_code = format_str.split('+')
            aformat = media.get_format_by_code(aformat_code)
            vformat = media.get_format_by_code(vformat_code)
            media.downloaded_format = vformat['format']
            media.downloaded_height = vformat['height']
            media.downloaded_width = vformat['width']
            media.downloaded_audio_codec = aformat['acodec']
            media.downloaded_video_codec = vformat['vcodec']
            media.downloaded_container = container
            media.downloaded_fps = vformat['fps']
            media.downloaded_hdr = vformat['is_hdr']
        else:
            # Combined stream or audio-only stream
            cformat_code = format_str
            cformat = media.get_format_by_code(cformat_code)
            media.downloaded_audio_codec = cformat['acodec']
            if cformat['vcodec']:
                # Combined
                media.downloaded_format = cformat['format']
                media.downloaded_height = cformat['height']
                media.downloaded_width = cformat['width']
                media.downloaded_video_codec = cformat['vcodec']
                media.downloaded_fps = cformat['fps']
                media.downloaded_hdr = cformat['is_hdr']
            else:
                media.downloaded_format = 'audio'
        media.save()
        # If selected, copy the thumbnail over as well
        if media.source.copy_thumbnails and media.thumb:
            log.info(f'Copying media thumbnail from: {media.thumb.path} '
                     f'to: {media.thumbpath}')
            copyfile(media.thumb.path, media.thumbpath)
        # If selected, write an NFO file
        if media.source.write_nfo:
            log.info(f'Writing media NFO file to: to: {media.nfopath}')
            write_text_file(media.nfopath, media.nfoxml)
        # Schedule a task to update media servers
        for mediaserver in MediaServer.objects.all():
            log.info(f'Scheduling media server updates')
            verbose_name = _('Request media server rescan for "{}"')
            rescan_media_server(
                str(mediaserver.pk),
                queue=str(media.source.pk),
                priority=0,
                verbose_name=verbose_name.format(mediaserver),
                remove_existing_tasks=True
            )
    else:
        # Expected file doesn't exist on disk
        err = (f'Failed to download media: {media} (UUID: {media.pk}) to disk, '
               f'expected outfile does not exist: {media.filepath}')
        log.error(err)
        # Raising an error here triggers the task to be re-attempted (or fail)
        raise DownloadFailedException(err)


@background(schedule=0)
def rescan_media_server(mediaserver_id):
    '''
        Attempts to request a media rescan on a remote media server.
    '''
    try:
        mediaserver = MediaServer.objects.get(pk=mediaserver_id)
    except MediaServer.DoesNotExist:
        # Task triggered but the media server no longer exists, do nothing
        return
    # Request an rescan / update
    log.info(f'Updating media server: {mediaserver}')
    mediaserver.update()
