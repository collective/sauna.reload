# -*- coding: utf-8 -*-
# Copyright (c) 2011 University of Jyväskylä and Contributors.
#
# All Rights Reserved.
#
# Authors:
#     Asko Soukka <asko.soukka@iki.fi>
#     Esa-Matti Suuronen <esa-matti@suuronen.org>
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS

"""Utilities for deferring product installations under selected paths"""

from Zope2.App.zcml import load_config


def is_sauna_product(product_dir):
    """
    Check if a product dir is reloadable.

    :param product_dir: Absolute path to an egg
    """
    from sauna.reload import reload_paths

    if reload_paths.hasAbsPath(product_dir):
        return True

    return False


# Monkey-patched to delay sauna.reloadable Products.xxx eggs loads
# by Zope 2.13.
def install_products(app):
    # Install a list of products into the basic folder class, so
    # that all folders know about top-level objects, aka products
    #
    from OFS.Application import get_folder_permissions
    from OFS.Application import install_product
    from OFS.Application import install_package
    from OFS.Application import get_packages_to_initialize
    from OFS.Application import get_products

    from App.config import getConfiguration
    import transaction

    folder_permissions = get_folder_permissions()
    meta_types = []
    done = {}

    debug_mode = getConfiguration().debug_mode

    transaction.get().note('Prior to product installs')
    transaction.commit()

    products = get_products()

    for priority, product_name, index, product_dir in products:
        # For each product, we will import it and try to call the
        # intialize() method in the product __init__ module. If
        # the method doesnt exist, we put the old-style information
        # together and do a default initialization.

        if product_name in done:
            continue

        if is_sauna_product(product_dir):
            # Will be later loaded by installDeferred()
            continue

        done[product_name] = 1
        install_product(app, product_dir, product_name, meta_types,
                        folder_permissions, raise_exc=debug_mode)

    # Delayed install of packages-as-products
    for module, init_func in tuple(get_packages_to_initialize()):
        install_package(app, module, init_func, raise_exc=debug_mode)


def findProducts():
    """
    Do not find products under sauna.reload's reload paths.
    """
    import Products
    from types import ModuleType
    from sauna.reload import reload_paths
    products = []
    for name in dir(Products):
        product = getattr(Products, name)
        if isinstance(product, ModuleType) and hasattr(product, '__file__'):
            # Do not find products under sauna.reload's reload paths
            if not getattr(product, '__file__') in reload_paths:
                products.append(product)
    return products


def findDeferredProducts():
    """
    Find only those products, which are under some reload path.
    """
    import Products
    from types import ModuleType
    from sauna.reload import reload_paths
    products = []
    for name in dir(Products):
        product = getattr(Products, name)
        if isinstance(product, ModuleType) and hasattr(product, '__file__'):
            # Find only products under sauna.reload's reload paths
            if getattr(product, '__file__') in reload_paths:
                products.append(product)
    return products


def deferInstalls():
    """
    Patch fiveconfigure with findProducts-method,
    which is unable to see the reloaded paths.
    """
    try:
        import OFS.metaconfigure
        import OFS.Application
        setattr(OFS.metaconfigure, 'findProducts', findProducts)
        setattr(OFS.Application, 'install_products', install_products)

    except ImportError:
        import Products.Five.fiveconfigure
        setattr(Products.Five.fiveconfigure, 'findProducts', findProducts)


def installDeferred():
    """
    Temporarily patch fiveconfigure with a findProducts-method,
    which is able to see only the products under the reload paths
    and execute Five configuration directives for those.
    """
    import sauna.reload
    try:
        # Zope 2.13
        import OFS.metaconfigure
        setattr(OFS.metaconfigure, 'findProducts', findDeferredProducts)
        load_config('fiveconfigure.zcml', sauna.reload)
        setattr(OFS.metaconfigure, 'findProducts', findProducts)
    except ImportError:
        # Zope 2.12
        import Products.Five.fiveconfigure
        setattr(Products.Five.fiveconfigure, 'findProducts',
                findDeferredProducts)
        load_config('fiveconfigure.zcml', sauna.reload)
        setattr(Products.Five.fiveconfigure, 'findProducts', findProducts)

    # Five pushes old-style product initializations into
    # Products._packages_to_initialize-list.
    # We must loop through that list to find the reloaded packages
    # and try to install them when found.
    from App.config import getConfiguration
    from OFS.Application import install_package
    from Zope2.App.startup import app
    app = app()
    debug_mode = getConfiguration().debug_mode
    from sauna.reload import reload_paths
    try:
        # Zope 2.13
        import OFS.metaconfigure
        # We iterate a copy of packages_to_initialize,
        # because install_package mutates the original.
        packages_to_initialize = [info for info in getattr(
            OFS.metaconfigure, '_packages_to_initialize', [])]
        for module, init_func in packages_to_initialize:
            if getattr(module, '__file__') in reload_paths:
                install_package(app, module, init_func, raise_exc=debug_mode)
    except ImportError:
        # Zope 2.12
        import Products
        # We iterate a copy of packages_to_initialize,
        # because install_package mutates the original.
        packages_to_initialize = [info for info in getattr(
            Products, '_packages_to_initialize', [])]
        for module, init_func in packages_to_initialize:
            if getattr(module, '__file__') in reload_paths:
                install_package(app, module, init_func, raise_exc=debug_mode)
        if hasattr(Products, '_packages_to_initialize'):
            del Products._packages_to_initialize
