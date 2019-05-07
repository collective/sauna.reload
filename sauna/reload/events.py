# -*- coding: utf-8 -*-
# Copyright (c) 2011 University of Jyväskylä and Contributors.
#
# All Rights Reserved.
#
# Authors:
#     Esa-Matti Suuronen <esa-matti@suuronen.org>
#     Asko Soukka <asko.soukka@iki.fi>
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS

"""Default Event Implementations"""

from zope.interface import implementer

from sauna.reload.interfaces import INewChildForked, INewChildIsReady


@implementer(INewChildForked)
class NewChildForked(object):

    def __init__(self, forkloop):
        self.forkloop = forkloop


@implementer(INewChildIsReady)
class NewChildIsReady(object):

    def __init__(self, forkloop):
        self.forkloop = forkloop
