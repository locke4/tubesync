import os
from django.conf import settings
from django.db.models.signals import pre_save, post_save, pre_delete, post_delete
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from background_task.signals import task_failed
from background_task.models import Task
from common.logger import log
from .models import Source, Media, MediaServer
from .tasks import (delete_task_by_source, delete_task_by_media, index_source_task,
                    download_media_thumbnail, download_media_metadata,
                    map_task_to_instance, check_source_directory_exists,
                    download_media, rescan_media_server)
from .utils import delete_file
#celery tasks, different to above import (TODO, remove from .tasks and add to .celerytasks perhaps?)
from .tasks import pre_update_source, post_update_source, post_media_save, pre_source_delete 

@receiver(pre_save, sender=Source)
def source_pre_save(sender, instance, **kwargs):
    # Triggered before a source is saved, if the schedule has been updated recreate
    # its indexing task
    

    try:
        existing_source = Source.objects.get(pk=instance.pk)
    except Source.DoesNotExist:
        # Probably not possible?
        return
    if existing_source.index_schedule != instance.index_schedule:
        pre_update_source.delay(instance.pk)
        # Indexing schedule has changed, recreate the indexing task

@receiver(post_save, sender=Source)
def source_post_save(sender, instance, created, **kwargs):
    # Check directory exists and create an indexing task for newly created sources
    post_update_source.delay(instance.pk, instance.name, created, instance.index_schedule)


@receiver(pre_delete, sender=Source)
def source_pre_delete(sender, instance, **kwargs):
    # Triggered before a source is deleted, delete all media objects to trigger
    # the Media models post_delete signal
    pre_source_delete.delay(instance.pk)


@receiver(post_delete, sender=Source)
def source_post_delete(sender, instance, **kwargs):
    # Triggered after a source is deleted
    log.info(f'Deleting tasks for source: {instance.name}')
    delete_task_by_source('sync.tasks.index_source_task', instance.pk)


@receiver(task_failed, sender=Task)
def task_task_failed(sender, task_id, completed_task, **kwargs):
    # Triggered after a task fails by reaching its max retry attempts
    obj, url = map_task_to_instance(completed_task)
    if isinstance(obj, Source):
        log.error(f'Permanent failure for source: {obj} task: {completed_task}')
        obj.has_failed = True
        obj.save()


@receiver(post_save, sender=Media)
def media_post_save(sender, instance, created, **kwargs):
    # If the media is skipped manually, bail.
    post_media_save.delay(instance.pk)
    if instance.manual_skip:
        return    
    

@receiver(pre_delete, sender=Media)
def media_pre_delete(sender, instance, **kwargs):
    # Triggered before media is deleted, delete any scheduled tasks
    log.info(f'Deleting tasks for media: {instance.name}')
    delete_task_by_media('sync.tasks.download_media', (str(instance.pk),))
    thumbnail_url = instance.thumbnail
    if thumbnail_url:
        delete_task_by_media('sync.tasks.download_media_thumbnail',
                             (str(instance.pk), thumbnail_url))


@receiver(post_delete, sender=Media)
def media_post_delete(sender, instance, **kwargs):
    # Schedule a task to update media servers
    for mediaserver in MediaServer.objects.all():
        log.info(f'Scheduling media server updates')
        verbose_name = _('Request media server rescan for "{}"')
        rescan_media_server(
            str(mediaserver.pk),
            priority=0,
            verbose_name=verbose_name.format(mediaserver),
            remove_existing_tasks=True
        )
