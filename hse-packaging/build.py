#!/usr/bin/env python3
#
#    SPDX-License-Identifier: AGPL-3.0-only
#
#    Copyright (C) 2017-2020 Micron Technology, Inc.
#
#    This code is derived from and modifies the MongoDB project.
#

import argparse
import os
import shutil
import subprocess
import tarfile
from getpass import getuser

import buildlib


DEVTOOLSET_ROOT = buildlib.DEVTOOLSET_ROOT
MONGO_ROOT = buildlib.MONGO_ROOT
VERSION = buildlib.get_hse_mongo_version()
MONGO_VERSION = buildlib.get_mongo_version()

DISTSRC_TARGETS = [
    'GNU-AGPL-3.0',
    'MPL-2',
    'README',
    'THIRD-PARTY-NOTICES',
]
SCONS_TARGETS = [
    'mongo',
    'mongobridge',
    'mongod',
    'mongos',
    'mongoperf',
]
TOOLS_BIN_DIR = 'src/mongo/gotools/bin'
TOOLS_TARGETS = [
    'bsondump',
    'mongostat',
    'mongofiles',
    'mongoexport',
    'mongoimport',
    'mongorestore',
    'mongodump',
    'mongotop',
    'mongooplog',
    'mongoreplay',
]


def parse_args():
    default_prefix = os.path.join(
        '/', 'tmp', getuser(), 'pkgbuild'
    )
    default_tarfile = os.path.join(
        MONGO_ROOT, 'hse-mongodb-linux-x86_64-%s.tgz' % VERSION
    )

    parser = argparse.ArgumentParser()

    grp = parser.add_argument_group('actions')
    grp = grp.add_mutually_exclusive_group()
    grp.add_argument('--packages', action='store_true',
                     help='Build binary tarball plus packages (default '
                          'action)')
    grp.add_argument('--tarball', action='store_true',
                     help='Build binary tarball only')
    grp.add_argument('--tarball-with-tests', action='store_true',
                     help='Build binary tarball only, but also include the '
                          'JavaScript test suites')

    grp = parser.add_argument_group('options')
    grp.add_argument('--build-number', '-b', type=int)
    grp.add_argument('--clean', '-c', action='store_true',
                     help='Clean all build targets before running')
    grp.add_argument('--distro', '-d',
                     help='Distro (as passed to buildscripts/packager.py). '
                          '(default: currently running distro)')
    grp.add_argument('--prefix', '-p', default=default_prefix,
                     help='Output directory prefix for package build '
                          '(default: %s)' % default_prefix)
    grp.add_argument('--rel-candidate', '-r', action='store_true',
                     help='Hide Git information in version strings')
    grp.add_argument('--strip', '-s', action='store_true',
                     help='Strip debugging symbols '
                          '(greatly reduces file sizes)')
    grp.add_argument('--tarfile', '-t', default=default_tarfile,
                     help='Tarball location (default: %s)' % default_tarfile)
    grp.add_argument('--verbose', '-v', action='store_true',
                     help='Verbose build output')

    # Hidden option to skip scons run and tarball generation, if possible.
    grp.add_argument('--script-test', action='store_true',
                     help=argparse.SUPPRESS)

    args = parser.parse_args()

    return args


def do_scons_and_tarball(args, with_tests=False):
    # init build options
    devtoolset_ok = buildlib.check_devtoolset_ok()
    ssl_ok = buildlib.check_ssl_ok()

    scons_flags = [
        '-j$(nproc)',
        '--disable-warnings-as-errors',
        '--dbg=off',
        '--opt=on',
    ]

    if ssl_ok:
        scons_flags.append('--ssl')

    if devtoolset_ok:
        scons_flags.extend([
            'CC=%s/bin/gcc' % DEVTOOLSET_ROOT,
            'CXX=%s/bin/g++' % DEVTOOLSET_ROOT,
            'OBJCOPY=%s/bin/objcopy' % DEVTOOLSET_ROOT,
        ])

    if with_tests:
        targets = SCONS_TARGETS + ['dbtest']
    else:
        targets = SCONS_TARGETS

    scons_flags += ['MONGO_VERSION=%s' % MONGO_VERSION]

    if args.verbose:
        scons_flags += ['VERBOSE=1']

    bin_paths = (
        [os.path.join(MONGO_ROOT, name) for name in targets] +
        [os.path.join(MONGO_ROOT, TOOLS_BIN_DIR, name) for name
         in TOOLS_TARGETS]
    )
    distsrc_paths = [
        os.path.join(MONGO_ROOT, 'distsrc', name)
        for name in DISTSRC_TARGETS
    ]

    # clean
    if args.clean:
        cmd = "rm -rf %s" % os.path.join(MONGO_ROOT, 'build/*')

        print(cmd)
        subprocess.check_call(cmd, cwd=MONGO_ROOT, shell=True)

        cmd = "rm -f %s" % " ".join(bin_paths)

        print(cmd)
        subprocess.check_call(cmd, cwd=MONGO_ROOT, shell=True)

    # run scons
    cmd = "buildscripts/scons.py %s" % " ".join(targets + scons_flags)

    print(cmd)
    subprocess.check_call(cmd, cwd=MONGO_ROOT, shell=True)

    # run tools build
    cmd = 'bash src/mongo/gotools/build.sh'

    print(cmd)
    subprocess.check_call(cmd, cwd=MONGO_ROOT, shell=True)

    # strip debug symbols, if requested
    if args.strip:
        for bin_path in bin_paths:
            cmd = 'strip --strip-debug %s' % bin_path

            print(cmd)
            subprocess.check_call(cmd, cwd=MONGO_ROOT, shell=True)

    # create tarball
    print("Creating archive at %s" % args.tarfile)

    tar_dir = 'hse-mongodb-linux-x86_64-%s' % VERSION

    with tarfile.open(args.tarfile, 'w:gz') as tar:
        for path in distsrc_paths:
            dest = os.path.join(
                tar_dir, os.path.basename(path)
            )

            print('adding to archive: %s -> %s' % (path, dest))
            tar.add(path, arcname=dest)

        for path in bin_paths:
            dest = os.path.join(
                tar_dir, 'bin', os.path.basename(path)
            )

            print('adding to archive: %s -> %s' % (path, dest))
            tar.add(path, arcname=dest)

        if with_tests:
            buildscripts_paths = [
                os.path.join(MONGO_ROOT, 'buildscripts', '__init__.py'),
                os.path.join(MONGO_ROOT, 'buildscripts', 'resmoke.py'),
                os.path.join(MONGO_ROOT, 'buildscripts', 'resmokeconfig'),
                os.path.join(MONGO_ROOT, 'buildscripts', 'resmokelib'),
            ]

            for path in buildscripts_paths:
                dest = os.path.join(
                    tar_dir, 'buildscripts', os.path.basename(path)
                )

                print('adding to archive: %s -> %s' % (path, dest))
                tar.add(path, arcname=dest)

            path = os.path.join(MONGO_ROOT, 'jstests')
            dest = os.path.join(tar_dir, 'jstests')

            print('adding to archive: %s -> %s' % (path, dest))
            tar.add(path, arcname=dest)

    print("%s done" % args.tarfile)
    pass


def do_packages(args):
    arch = 'x86_64'

    if args.distro:
        distro = args.distro
    else:
        distro = buildlib.get_own_distro()

    pkg_type = buildlib.get_pkg_type(distro)

    os.makedirs(args.prefix, exist_ok=True)
    for dirname in ['dl', 'dst', 'repo', 'rpmbuild']:
        shutil.rmtree(os.path.join(args.prefix, dirname), ignore_errors=True)
    try:
        os.unlink(os.path.join(args.prefix, 'macros'))
    except FileNotFoundError:
        pass

    if args.script_test:
        print()
        print('Looking for tarball at %s...' % args.tarfile)
        if not os.path.exists(args.tarfile):
            print()
            print('%s not found, building...' % args.tarfile)
            do_scons_and_tarball(args, with_tests=True)
    else:
        do_scons_and_tarball(args, with_tests=True)

    print()
    print('Building packages for %s...' % distro)
    print()

    hse_mongo_sha = buildlib.get_hse_mongo_sha()
    hse_sha = buildlib.get_hse_sha()
    hse_version = buildlib.get_hse_version()

    script_dir = os.path.join(MONGO_ROOT, 'buildscripts')
    os.chdir(script_dir)

    cmd = 'python2 packager.py -s %s -t %s -d %s -a %s' % (
        VERSION, args.tarfile, distro, arch
    )

    if args.build_number:
        cmd += ' --build-number %d' % args.build_number

    if args.rel_candidate:
        cmd += ' --rel-candidate'

    cmd += ' --hse-mongo-sha %s --hse-sha %s --hse-version %s' % (
        hse_mongo_sha, hse_sha, hse_version,
    )

    cmd += ' -p %s' % args.prefix

    print(cmd)
    subprocess.check_call(cmd.split(), cwd=script_dir)

    if pkg_type == 'deb':
        cmd = 'find %s -name *.deb' % os.path.join(args.prefix, 'repo')
    else:
        cmd = 'find %s -name *.rpm' % os.path.join(args.prefix, 'repo')

    print()
    print(cmd)
    subprocess.check_call(cmd.split())


def main():
    args = parse_args()

    if args.tarball:
        do_scons_and_tarball(args)
    elif args.tarball_with_tests:
        do_scons_and_tarball(args, with_tests=True)
    else:
        do_packages(args)


if __name__ == '__main__':
    main()
