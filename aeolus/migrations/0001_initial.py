# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2020-02-06 15:13
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('coverages', '0007_typemodels'),
        ('backends', '0003_nameblank'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('identifier', models.CharField(max_length=256)),
                ('process_id', models.CharField(max_length=256)),
                ('response_url', models.CharField(max_length=512)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('started', models.DateTimeField(null=True)),
                ('stopped', models.DateTimeField(null=True)),
                ('status', models.CharField(choices=[('A', 'ACCEPTED'), ('R', 'STARTED'), ('S', 'SUCCEEDED'), ('T', 'ABORTED'), ('F', 'FAILED'), ('U', 'UNDEFINED')], default='U', max_length=1)),
                ('owner', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='jobs', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'WPS Job',
                'verbose_name_plural': 'WPS Jobs',
            },
        ),
        migrations.CreateModel(
            name='OptimizedProductDataItem',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('location', models.CharField(max_length=1024)),
                ('format', models.CharField(blank=True, max_length=64, null=True)),
                ('product', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='optimized_data_item', to='coverages.Product')),
                ('storage', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='backends.Storage')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='UserCollectionLink',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('collection', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='user_collection', to='coverages.Collection')),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='user_collection', to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]
