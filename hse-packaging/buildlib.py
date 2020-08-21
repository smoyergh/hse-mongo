#
#    SPDX-License-Identifier: AGPL-3.0-only
#
#    Copyright (C) 2017-2020 Micron Technology, Inc.
#
#    This code is derived from and modifies the MongoDB project.
#
import os.path
import re
import subprocess
import sys


DEVTOOLSET_ROOT = '/opt/rh/devtoolset-9/root'
MONGO_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)), '..'
    )
)


def check_devtoolset_ok():
    enable_script = os.path.join(DEVTOOLSET_ROOT, '..', 'enable')
    if os.path.isfile(enable_script):
        return True


def check_ssl_ok():
    os_id, os_version_id = get_os_release()

    #
    # Supporting OpenSSL 1.1.x on MongoDB 3.4 requires upstream code that we
    # have not applied because it was published after the SSPL took effect.
    #
    # RHEL 7 is still on OpenSSL 1.0.2, so it still works.
    #
    # TODO: check the OpenSSL version in a distro independent way.
    #
    try:
        return os_id == 'rhel' and float(os_version_id) < 8.0
    except ValueError:
        return False


def get_hse_mongo_sha():
    try:
        out = subprocess.check_output(
            'git rev-parse --short=7 HEAD', cwd=MONGO_ROOT, shell=True
        ).strip().decode()

        return out
    except subprocess.CalledProcessError:
        return 'nogit'


def get_hse_mongo_version():
    hse_conn_version = '0.0'
    mongo_version_sup = '0.0.0'

    with open('%s/src/mongo/db/storage/hse/VERSION' % MONGO_ROOT, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('HSE_CONN_VERSION'):
                hse_conn_version=line.split('=')[-1].strip()
            elif line.startswith('MONGO_VERSION_SUP'):
                mongo_version_sup=line.split('=')[-1].strip()

    return '{mvs}.{hcv}'.format(mvs=mongo_version_sup, hcv=hse_conn_version)


def get_hse_sha():
    blob = subprocess.check_output("hse -Vv", shell=True).strip().decode()
    for line in blob.splitlines():
        if line.startswith('sha:'):
            return line.split()[1][:7]

    return "nogit"


def get_hse_version():
    hse_min_version_sup = '0.0'
    with open('%s/src/mongo/db/storage/hse/VERSION' % MONGO_ROOT, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('HSE_MIN_VERSION_SUP'):
                hse_min_version_sup=line.split('=')[-1].strip()
    return hse_min_version_sup


def get_mongo_version():
    mongo_version_sup = '0.0.0'

    with open('%s/src/mongo/db/storage/hse/VERSION' % MONGO_ROOT, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('MONGO_VERSION_SUP'):
                mongo_version_sup=line.split('=')[-1].strip()

    return mongo_version_sup


def get_pkg_type(distro):
    # borrowed from make_pkg in buildscripts/packager.py
    if re.search("^(debian|ubuntu)", distro):
        return 'deb'
    elif re.search("^(suse|centos|redhat|rhel|fedora|amazon)", distro):
        return 'rpm'
    else:
        raise Exception("BUG: unsupported platform?")


def get_os_release():
    os_id = subprocess.check_output(
        ". /etc/os-release && echo $ID", shell=True
    )
    os_version_id = subprocess.check_output(
        ". /etc/os-release && echo $VERSION_ID", shell=True
    )

    return os_id, os_version_id


def get_own_distro():
    # this happens to work for both RHEL and Ubuntu
    out = subprocess.check_output(
        ". /etc/os-release && echo $ID$VERSION_ID", shell=True
    ).strip().decode()

    distro = out.replace('.', '')

    return distro
