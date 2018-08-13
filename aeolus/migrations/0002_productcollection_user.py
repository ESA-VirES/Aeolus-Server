# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('aeolus', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='productcollection',
            name='user',
            field=models.OneToOneField(related_name='user_collection', null=True, default=None, blank=True, to=settings.AUTH_USER_MODEL),
        ),
    ]
