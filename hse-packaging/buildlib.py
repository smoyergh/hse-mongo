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
    with open('%s/src/mongo/db/storage/hse/VERSION' % MONGO_ROOT, 'r') as fd:
        version = fd.read().strip()

    return version


def get_hse_sha():
    return "nogit"  # FIXME


def get_hse_version():
    try:
        version_str = subprocess.check_output(
            "rpm --queryformat='%{VERSION}' -q hse-devel", shell=True
        ).strip().decode()
    except subprocess.CalledProcessError as cpe:
        try:
            version_str = subprocess.check_output(
                "dpkg-query --showformat '${Version}' -W hse-devel", shell=True
            ).strip().decode()
        except subprocess.CalledProcessError:
            print('hse-devel is not installed')
            sys.exit(1)

    # assuming x.y.z and ignoring anything after
    match = re.match(r'^(\d+\.\d+\.\d+)', version_str)
    version = match.group(1)

    return version


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
