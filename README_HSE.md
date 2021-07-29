# MongoDB with HSE

This fork of MongoDB&reg; integrates HSE with MongoDB 3.4.17.  The goal is
to demonstrate the benefits of HSE within a real-world storage application.

The reader is assumed to be familiar with configuring and running MongoDB,
as well as HSE concepts and terminology.
The information provided here is specific to using MongoDB with HSE.


## Installing HSE

Clone the [hse repo](https://github.com/hse-project/hse)
and follow the documentation in `README.md` to build and install HSE.

You must use HSE version 2.0 or higher.


## Installing MongoDB Dependencies

> TODO: This needs consideration.  The goal is that contributors will
> port to many platforms and distros, and it is impractical to identify
> all the resulting dependencies.  What Alex and I discussed as an idea
> is to provide a list for a few distros (e.g, RHEL 8, Ubuntu 18), and let
> users figure it out for their specific platform from there.


## Installing MongoDB with HSE

Clone the [hse-mongo repo](https://github.com/hse-project/hse-mongo)
and checkout the latest release tag.  Releases are named `rA.B.C.D.E-hse` where

* `A.B.C` is the MongoDB version (e.g., 3.4.17)
* `D.E` is our MongoDB integration version

For example

    $ git clone https://github.com/hse-project/hse-mongo.git
    $ cd hse-mongo
    $ git checkout rA.B.C.D.E-hse

Build with the following command

    $ hse-packaging/build.py --clean


> TODO: Change to native Scons build?  Document manual install?


## Configuring MongoDB Options

MongoDB with HSE adds the following command-line options to `mongod`,
which are reflected in `mongod --help`.

* `--hseStagingPath` is the directory path for the optional staging media class; default is none
* `--hseCompression` is the compression algorithm applied (`lz4` or `none`); default is `lz4`
* `--hseCompressionMinBytes` is the min document size in bytes to compress; default is `0`

These HSE options are also supported in `mongod.conf`, in addition
to the standard storage configuration options, as in the following example.

    # Standard options
    storage:
      dbPath: /var/lib/mongo/myDB
      journal:
        enabled: true
        commitIntervalMs: 100

    # Use Heterogeneous-memory Storage Engine (HSE). This is the default.
      engine: hse
      hse:

    # Uncomment to customize compression for HSE.
    # Allowable compression types are "lz4" or "none". Default is "lz4".
    # Minimum document size to compress in bytes.  Default is zero (0).
    #    compression: none
    #    compressionMinBytes: 0

    # Uncomment to create the optional staging media class.  Default is none.
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


## MongoDB Data Storage

All MongoDB data is stored in an HSE KVDB.  The parameter `dbPath` specifies
the MongoDB data directory.  The first time `mongod` starts it creates
a KVDB with home directory `<dbPath>/hse` and capacity media class
`<dbPath>/hse/capacity`.

A staging media class can be configured at the time the KVDB is created,
or added later, via the command-line option `--hseStagingPath` or the
mongod.conf option `storage.hse.stagingPath`.  If the staging media class
already exists, these options have no effect.


## Running MongoDB with HSE

Start and manage `mongod` as you would normally.

This version of MongoDB with HSE does not support the following:

* `compact` administration command
* `fsync` administration command with the lock option, or the
corresponding `fsyncUnlock` command
* Read concern "majority"
* SSL

