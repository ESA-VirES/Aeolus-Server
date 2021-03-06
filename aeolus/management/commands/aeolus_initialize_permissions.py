# ------------------------------------------------------------------------------
#
# Project: EOxServer <http://eoxserver.org>
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2019 EOX IT Services GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies of this Software or works derived from this Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# ------------------------------------------------------------------------------

from django.core.management.base import BaseCommand
from django.contrib.auth import models as auth
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from eoxserver.resources.coverages.management.commands import CommandOutputMixIn
from eoxserver.resources.coverages.models import Collection


class Command(CommandOutputMixIn, BaseCommand):
    @transaction.atomic
    def handle(self, *args, **kwargs):
        content_type = ContentType.objects.get_for_model(Collection)

        for collection in Collection.objects.all():
            _, created = auth.Permission.objects.get_or_create(
                codename='access_%s' % collection.identifier,
                name='Can access collection %s' % collection.identifier,
                content_type=content_type,
            )

            if created:
                self.print_msg(
                    "Created permission for collection '%s'"
                    % collection.identifier
                )
            else:
                self.print_msg(
                    "Permission for collection '%s' already exists"
                    % collection.identifier
                )

        # default group does not have access to AUX collections
        group, created = auth.Group.objects.get_or_create(
            name='aeolus_default'
        )
        if created:
            # get permissions for "open" collections
            permissions = auth.Permission.objects.filter(
                codename__startswith='access_'
            ).exclude(
                codename__startswith='access_AUX'
            ).exclude(
                codename__startswith='access_user_collection'
            )
            group.permissions = permissions
            group.save()

            self.print_msg("Created group %s" % group.name)

            for user in auth.User.objects.all():
                user.groups.add(group)
                self.print_msg(
                    "Added user %s group %s" % (user.username, group.name)
                )

        # privileged group has access to all collections
        group, created = auth.Group.objects.get_or_create(
            name='aeolus_privileged'
        )
        if created:
            # get permissions for "open" collections
            permissions = auth.Permission.objects.filter(
                codename__startswith='access_'
            ).exclude(
                codename__startswith='access_user_collection'
            )
            group.permissions = permissions
            group.save()
            self.print_msg("Created group %s" % group.name)

        # give each user access to his own user collection
        for user in auth.User.objects.all():
            user.user_permissions.add(
                auth.Permission.objects.get(
                    codename='access_user_collection_%s' % user.username
                )
            )
