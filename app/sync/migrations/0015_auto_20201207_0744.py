# Generated by Django 3.1.4 on 2020-12-07 07:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('sync', '0014_source_has_errors'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='source',
            name='has_errors',
        ),
        migrations.AddField(
            model_name='source',
            name='has_failed',
            field=models.BooleanField(default=False, help_text='Source has failed to index media', verbose_name='has failed'),
        ),
    ]
