#!/usr/bin/python3

#
#    SPDX-License-Identifier: AGPL-3.0-only
#
#    Copyright (C) 2017-2020 Micron Technology, Inc.
#
#    This code is derived from and modifies the mongo-rocks project.
#
#    Copyright (C) 2014 MongoDB Inc.
#
#    This program is free software: you can redistribute it and/or  modify
#    it under the terms of the GNU Affero General Public License, version 3,
#    as published by the Free Software Foundation.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#    As a special exception, the copyright holders give permission to link the
#    code of portions of this program with the OpenSSL library under certain
#    conditions as described in each individual source file and distribute
#    linked combinations including the program with the OpenSSL library. You
#    must comply with the GNU Affero General Public License in all respects for
#    all of the code used other than as permitted herein. If you modify file(s)
#    with this exception, you may extend this exception to your version of the
#    file(s), but you are not obligated to do so. If you do not wish to do so,
#    delete this exception statement from your version. If you delete this
#    exception statement from all source files in the program, then also delete
#    it in the license file.
#

#
# Run the unit tests for the HSE connector, parse the output to get results,
# and pickle it to compare to prior runs to spot regressions.
#
# Limitations:  expects to be in the same directory as the unit test
# executables, and expects PWD to also be that directory.  Fortunately, because
# the unit test executables are statically-linked, we can scp this script and
# the binaries to a directory on a victim machine (or a test VM), run 'em, and
# slurp the pickled results back.

import os
import pickle
import pprint
import sys

from subprocess import Popen, PIPE

_HSEBIN = '/opt/hse-1/bin/hse1'
_KVDB_NAME = 'kvdb1'
_DEVICE_LIST = []

def check_for_tests(pwd, tests):
    missing = list()

    for t in tests:
        if not os.path.exists(os.path.join(pwd, t)):
            missing.append(t)

    if len(missing):
        raise FileNotFoundError(','.join(missing))

def _run_cmd(cmdargs, logfile):
    exit_code = 0
    proc = Popen(cmdargs, shell=True, stdout=logfile, stderr=logfile)
    exit_code = proc.wait()
    logfile.flush()
    return exit_code

def test_setup(pwd):
    fname = '%s/setup.out' % (pwd)
    logfile = open(fname, 'w')
    devlist_str = ' '.join(_DEVICE_LIST)

    harness_vg = 'harness_vg'
    harness_lv = 'harness_lv'

    cmdargs = [ 'vgremove -y %s' % (harness_vg), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)
    for dev in _DEVICE_LIST:
        cmdargs = [ 'pvcreate %s' % (dev), '>>', fname, '2>&1']
        _run_cmd(cmdargs, logfile)
    cmdargs = [ 'vgcreate %s %s' % (harness_vg, devlist_str), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    cmdargs = [ 'lvcreate -y -l 100%FREE --stripes {} --name {} {}'.format(len(_DEVICE_LIST), harness_lv, harness_vg), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    cmdargs = [ 'mkfs -t ext4 -F /dev/%s/%s' % (harness_vg, harness_lv), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    cmdargs = [ 'mkdir -p %s/%s && mount -onoatime /dev/%s/%s %s/%s' % (pwd, _KVDB_NAME, harness_vg, harness_lv, pwd, _KVDB_NAME), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    os.environ["HSE_STORAGE_PATH"] = '%s/%s' % (pwd, _KVDB_NAME)
    os.environ["HSE_REST_SOCK_PATH"] = '%s/%s' % (pwd, _KVDB_NAME)

    logfile.close()

def test_teardown(pwd):
    fname = '%s/teardown.out' % (pwd)
    logfile = open(fname, 'w')

    harness_vg = 'harness_vg'

    cmdargs = [ 'umount %s/%s && rmdir %s/%s' % (pwd, _KVDB_NAME, pwd, _KVDB_NAME), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    cmdargs = [ 'vgremove -y %s' % (harness_vg), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    logfile.close()

def run_test(pwd, test):
    fname = '%s/%s.out' %(pwd, test)
    logfile = open(fname, 'w')

    exit_code = 0

    cmdargs = [ '%s kvdb create %s' % (_HSEBIN, _KVDB_NAME), '>>', fname, '2>&1']
    exit_code = _run_cmd(cmdargs, logfile)
    if exit_code != 0:
        return exit_code

    cmdargs = ['%s/%s' % (pwd, test), '>>', fname, '2>&1']
    exit_code = _run_cmd(cmdargs, logfile)

    cmdargs = [ 'rm -f %s/%s/.lockfile && %s kvdb destroy %s' % (pwd, _KVDB_NAME, _HSEBIN, _KVDB_NAME), '>>', fname, '2>&1']
    _run_cmd(cmdargs, logfile)

    logfile.close()

    return exit_code

def parse_output(pwd, test):
    outfilename = '%s/%s.out' % (pwd, test)
    failures = set()

    with open(outfilename) as outfile:
        reached_failures = False
        for line in outfile.readlines():
            if line.find("Failing tests") == -1 and not reached_failures:
                continue
            elif line.find("Failing tests") != -1:
                reached_failures = True
                continue
            else:
                if line.find("FAILURE") == -1:
                    failures.add(line.split()[4])

    print("failures for %s: %s" % (t, failures))
    return failures


if __name__ == '__main__':
    usage = "Usage: hse_test_harness build_number hse_bin_path kvdb_name device1 device2 ..."

    if len(sys.argv) < 4:
        print("Error:  incorrect number of arguments!", file=sys.stderr)
        print(usage, file=sys.stderr)
        sys.exit(1)

    build_number = sys.argv[1]
    _HSEBIN = sys.argv[2]
    _KVDB_NAME = sys.argv[3]
    _DEVICE_LIST = sys.argv[4:]

    pwd = os.getcwd()

    tests = [
        'storage_hse_engine_test',
        'storage_hse_index_test',
        'storage_hse_record_store_test',
        'storage_hse_test'
    ]

    check_for_tests(pwd, tests)

    results = dict()

    exit_status = 0

    test_setup(pwd)

    for t in tests:
        exit_status += int(run_test(pwd, t))
        results[t] = parse_output(pwd, t)

    print("Pickling results")
    pickle.dump(results, open('%s/%s.db' % (pwd, build_number), 'wb'))

    print("Results:")
    pprint.pprint(results)
    print(exit_status)

    if exit_status == 0:
        test_teardown(pwd)

    sys.exit(exit_status)
