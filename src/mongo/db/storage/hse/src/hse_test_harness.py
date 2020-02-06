#!/usr/bin/python3
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

_NFBIN = '/usr/bin/nf'
_MPOOL_NAME = 'mp1'
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

def run_test(pwd, test):
    logfile = open('%s/%s.out' % (pwd, test), 'w')
    devlist_str = ' '.join(_DEVICE_LIST)

    # Reset mpool
    cmdargs = [ '%s mpool umount %s' % (_NFBIN, _MPOOL_NAME), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    _run_cmd(cmdargs, logfile)
    cmdargs = [ '%s mpool destroy %s' % (_NFBIN, _MPOOL_NAME), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    _run_cmd(cmdargs, logfile)

    harness_vg = 'harness_vg'
    harness_lv = 'harness_lv'

    cmdargs = [ 'vgremove -y %s' % (harness_vg), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    _run_cmd(cmdargs, logfile)
    for dev in _DEVICE_LIST:
        cmdargs = [ 'pvcreate %s' % (dev), '>>', '%s/%s.out' % (pwd, test), '2>&1']
        _run_cmd(cmdargs, logfile)
    cmdargs = [ 'vgcreate %s %s' % (harness_vg, devlist_str), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    _run_cmd(cmdargs, logfile)

    cmdargs = [ 'lvcreate -y -l 100%FREE --stripes {} --name {} {}'.format(len(_DEVICE_LIST), harness_lv, harness_vg), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    _run_cmd(cmdargs, logfile)

    exit_code = 0
    cmdargs = [ '%s device prepare -f /dev/%s/%s' % (_NFBIN, harness_vg, harness_lv), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    exit_code = _run_cmd(cmdargs, logfile)
    if exit_code != 0:
        return exit_code

    cmdargs = [ '%s mpool create %s /dev/%s/%s' % (_NFBIN, _MPOOL_NAME, harness_vg, harness_lv), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    exit_code = _run_cmd(cmdargs, logfile)
    if exit_code != 0:
        return exit_code
    cmdargs = [ '%s mpool mount %s' % (_NFBIN, _MPOOL_NAME), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    exit_code = _run_cmd(cmdargs, logfile)
    if exit_code != 0:
        return exit_code
    cmdargs = ['%s/%s' % (pwd, test), '>>', '%s/%s.out' % (pwd, test), '2>&1']
    exit_code = _run_cmd(cmdargs, logfile)
    if exit_code != 0:
        return exit_code

    # Run test
    cmdargs = ['%s/%s' % (pwd, test), '>', '%s/%s.out' % (pwd, test), '2>&1']
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
    usage = "Usage: hse_test_harness build_number mpool_name device1 device2 ..."

    if len(sys.argv) < 4:
        print("Error:  incorrect number of arguments!", file=sys.stderr)
        print(usage, file=sys.stderr)
        sys.exit(1)

    build_number = sys.argv[1]
    _NFBIN = sys.argv[2]
    _MPOOL_NAME = sys.argv[3]
    _DEVICE_LIST = sys.argv[4:]

    pwd = os.getcwd()

    tests = [
        'storage_mse_kvdb_engine_test',
        'storage_mse_kvdb_index_test',
        'storage_mse_kvdb_record_store_test',
        'storage_mse_kvdb_test'
    ]

    check_for_tests(pwd, tests)

    results = dict()

    exit_status = 0
    for t in tests:
        exit_status += int(run_test(pwd, t))
        results[t] = parse_output(pwd, t)

    print("Pickling results")
    pickle.dump(results, open('%s/%s.db' % (pwd, build_number), 'wb'))

    print("Results:")
    pprint.pprint(results)
    print(exit_status)
    sys.exit(exit_status)
