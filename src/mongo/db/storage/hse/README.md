# MongoDB with HSE

This fork of MongoDB&reg; integrates HSE with MongoDB 3.4.17.  The goal is
to demonstrate the benefits of HSE within a real-world storage application.
This version of MongoDB is not intended for production environments.

The reader is assumed to be familiar with configuring and running MongoDB,
as well as HSE concepts and terminology.
The information provided here is specific to using MongoDB with HSE.


## Installing HSE

Clone the [`hse`](https://github.com/hse-project/hse) repo
and follow the documentation in
[`README.md`](https://github.com/hse-project/hse/blob/master/README.md)
to build and install HSE.

You must use HSE version 2.0 or higher.


## Installing MongoDB Dependencies

Depending on your Linux distribution and environment, you may need to
install additional packages to build MongoDB.
For example, building MongoDB requires

* GCC 5.3.0 (or newer) or Clang 3.4 (or newer)
* Python 2.7
* SCons 2.3

To help you with this process, below are examples of the packages required
for several common Linux distributions.  These are **in addition to**
the packages required to build HSE.

### RHEL 8 Packages

```shell
sudo dnf install lz4-devel
sudo alternatives --set python /usr/bin/python2
pip2 install --user scons
```

### Ubuntu 18.04 Packages

```shell
sudo apt install scons liblz4-dev
```


## Installing MongoDB with HSE

Clone the [`hse-mongo`](https://github.com/hse-project/hse-mongo) repo
and checkout the latest release tag.  Releases are named `rA.B.C.D.E-hse` where

* `A.B.C` is the MongoDB version (e.g., 3.4.17)
* `D.E` is our MongoDB integration version

For example

```shell
git clone https://github.com/hse-project/hse-mongo.git
cd hse-mongo
git checkout rA.B.C.D.E-hse
```

Build MongoDB with HSE as follows.

```shell
scons -j $(nproc) --disable-warnings-as-errors CPPPATH=/opt/hse/include/hse-2 LIBPATH=/opt/hse/lib64 mongod mongos mongo
```

The resulting binaries are stored in directory `./build/opt/mongo`.

> The `CPPPATH` and `LIBPATH` paths depend on both where you installed HSE
> and your Linux distribution.  You need to locate these directories to
> set these variables correctly.

Build and run unit tests as follows.

```shell
scons -j$(nproc) --dbg=off --opt=on CPPPATH=/opt/hse/include/hse-2 LIBPATH=/opt/hse/lib64 --disable-warnings-as-errors hse_unit_tests
cd build/opt/mongo/db/storage/hse
mkdir kvdb_home_test
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/hse/lib64
./hse_test_harness.py 0 /opt/hse/bin/hse $(realpath ./kvdb_home_test)
```

## Configuring MongoDB Options

MongoDB with HSE adds the following command-line options to `mongod`,
which are reflected in `mongod --help`.

* `--hseStagingPath` is the directory path for the optional staging media class; default is none
* `--hseCompression` is the compression algorithm applied (`lz4` or `none`); default is `lz4`
* `--hseCompressionMinBytes` is the min document size in bytes to compress; default is `0`

These HSE options are also supported in `mongod.conf`, in addition
to the standard storage configuration options, as in the following example.

```yaml
# Standard options
storage:
  dbPath: /var/lib/mongo
  journal:
    enabled: true
    commitIntervalMs: 100

# Use Heterogeneous-memory Storage Engine (HSE). This is the default.
  engine: hse

# Uncomment the following to customize HSE configuration options
#  hse:

# Allowable compression types are "lz4" or "none". Default is "lz4".
# Minimum document size to compress in bytes.  Default is zero (0).
#    compression: none
#    compressionMinBytes: 0

# Create the optional staging media class.  Default is none.
#    stagingPath:

# Recommended oplog size for HSE when using replica sets.
replication:
  oplogSizeMB: 32000
  replSetName: rs1

# Recommended query and other parameters for HSE
setParameter:
  internalQueryExecYieldIterations: 100000
  internalQueryExecYieldPeriodMS: 1000
  replWriterThreadCount: 64
```

## MongoDB Data Storage

The MongoDB configuration option `dbPath` specifies the MongoDB data directory.
All MongoDB data is stored in an HSE KVDB.  The first time `mongod` starts
it creates a KVDB with home directory `<dbPath>/hse` and capacity media class
`<dbPath>/hse/capacity`.

An optional staging media class can be configured at the time `mongod` creates
a KVDB.  The staging media class directory can be specified via the
command-line option `--hseStagingPath` or the mongod.conf option
`storage.hse.stagingPath`.


## Running MongoDB with HSE

Start and manage `mongod` as you would normally.

This version of MongoDB with HSE does not support the following:

* `compact` administration command
* `fsync` administration command with the lock option, or the
corresponding `fsyncUnlock` command
* Read concern "majority"
* `storage.directoryPerDB` configuration value of `true`
* SSL on some platforms, which is unrelated to HSE.  E.g., RHEL 8 and
Ubuntu 18.04.


## Storage and Benchmarking Tips

Please see the HSE [project documentation](https://hse-project.github.io/)
for information on configuring HSE storage and running benchmarks.
It contains important details on HSE file system requirements, configuration
options, performance tuning, and best practices for benchmarking.
