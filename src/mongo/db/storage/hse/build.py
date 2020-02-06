import os
import subprocess

K_CON_SRC = os.path.dirname(os.path.abspath(__file__))
K_CON_VERSION_FILE = os.path.join(K_CON_SRC, 'VERSION')
K_GEN_FILE_NAME = os.path.join(K_CON_SRC, 'src', 'hse_versions.h')

def has_option(env, name):
    """ Lifted from mongo SConstruct """
    optval = env.GetOption(name)
    #
    # Options with nargs=0 are true when their value is the empty tuple.
    # Otherwise, if the value is falsish (empty string, None, etc.), coerce to
    # False.
    #
    return True if optval == () else bool(optval)

def getMseGitSha(conf, env):
    #
    # Get the version string from the mse-devel RPM.
    #
    # rpm -qa is always successful(ret code 0)
    version = subprocess.check_output(['rpm', '-qa', 'hse-devel'])

    if len(version) == 0:
        retval = "DEVBRANCH"
    else:
        retval = '-'.join(version.strip().decode('utf-8').split('-')[2:])

    return retval

def getMseConnectorGitSha(conf, env):
    s_pwd = os.getcwd()
    print s_pwd
    os.chdir(K_CON_SRC)
    sha=subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    os.chdir(s_pwd)
    return sha.strip().decode('utf-8')

def getMseVersion(conf, env):
    mse_ver = 'DEVBRANCH'
    try:
        rpm_info = subprocess.check_output(['rpm', '-qi', 'hse-devel'])
        mse_ver = [y.split(':')[-1] for y in rpm_info.split('\n') if y.startswith('Version')][0].strip(' ')
    except:
        pass

    return mse_ver

def getMseConnectorVersion(conf, env):
    line = 'UNKNOWN'
    with open(K_CON_VERSION_FILE, 'r') as f:
        line = f.readline()
    return line.strip()

def gensrc(conf, env):
    with open(K_GEN_FILE_NAME, 'w') as f:
        contents = \
'''#pragma once

namespace hse {{
static const char* K_HSE_VERSION="{mse_version}";
static const char* K_HSE_CONNECTOR_VERSION="{connector_version}";
static const char* K_HSE_GIT_SHA="{mse_git_sha}";
static const char* K_HSE_CONNECTOR_GIT_SHA="{mse_connector_git_sha}";
}}'''.format(
            mse_version=getMseVersion(conf, env),
            connector_version=getMseConnectorVersion(conf, env),
            mse_git_sha=getMseGitSha(conf, env),
            mse_connector_git_sha=getMseConnectorGitSha(conf, env)
        );
        f.write(contents)

def configure(conf, env):
    print("Configuring NFE MongoDB storage engine module")
    gensrc(conf, env)
