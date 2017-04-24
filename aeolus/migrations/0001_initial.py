# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
from django.conf import settings
import django.contrib.gis.db.models.fields


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('coverages', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('identifier', models.CharField(max_length=256)),
                ('process_id', models.CharField(max_length=256)),
                ('response_url', models.CharField(max_length=512)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('started', models.DateTimeField(null=True)),
                ('stopped', models.DateTimeField(null=True)),
                ('status', models.CharField(default=b'U', max_length=1, choices=[(b'A', b'ACCEPTED'), (b'R', b'STARTED'), (b'S', b'SUCCEEDED'), (b'T', b'ABORTED'), (b'F', b'FAILED'), (b'U', b'UNDEFINED')])),
                ('owner', models.ForeignKey(related_name='jobs', blank=True, to=settings.AUTH_USER_MODEL, null=True)),
            ],
            options={
                'verbose_name': 'WPS Job',
                'verbose_name_plural': 'WPS Jobs',
            },
        ),
        migrations.CreateModel(
            name='Product',
            fields=[
                ('coverage_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='coverages.Coverage')),
                ('ground_path', django.contrib.gis.db.models.fields.MultiLineStringField(srid=4326, null=True, blank=True)),
            ],
            options={
                'abstract': False,
            },
            bases=('coverages.coverage',),
        ),
        migrations.CreateModel(
            name='ProductCollection',
            fields=[
                ('collection_ptr', models.OneToOneField(parent_link=True, auto_created=True, to='coverages.Collection')),
                ('product_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='aeolus.Product')),
            ],
            options={
                'verbose_name': 'Product Collection',
                'verbose_name_plural': 'Product Collections',
            },
            bases=('aeolus.product', 'coverages.collection'),
        ),
    ]
