#-------------------------------------------------------------------------------
#
#  Handling VirES OAuth user permissions.
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2021 EOX IT Services GmbH
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
#-------------------------------------------------------------------------------

from logging import getLogger
from django.contrib.auth.models import Group
from allauth.socialaccount import app_settings
from eoxs_allauth.vires_oauth.provider import ViresProvider


def update_user_groups(social_account):
    """ Update user's groups according to the user's permissions stored
    in the social account object.
    """
    logger = getLogger(__name__)
    user = social_account.user
    vires_permissions = get_vires_permissions(social_account)
    required_group_permissions = get_required_group_permissions()
    for group in Group.objects.filter(name__in=list(required_group_permissions)):
        permission = required_group_permissions[group.name]
        if permission in vires_permissions:
            group.user_set.add(user)
            logger.debug("user %s added to group %s", user.username, group.name)
        else:
            group.user_set.remove(user)
            logger.debug("user %s removed from group %s", user.username, group.name)


def get_vires_permissions(social_account):
    """ Extract VirES user permissions form the social account object. """
    return set(social_account.extra_data.get('permissions', []))


def get_required_group_permissions():
    """ Get the configured required Aeolus user group VirES permissions. """
    return (
        app_settings.PROVIDERS.get(ViresProvider.id) or {}
    ).get("REQUIRED_GROUP_PERMISSIONS") or {}
