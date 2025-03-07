#
#    SPDX-License-Identifier: AGPL-3.0-only
#
#    Copyright (C) 2017-2020 Micron Technology, Inc.
#
#    This code is derived from and modifies the MongoDB project.
#

"""
Standalone mongod fixture for executing JSTests against.
"""

from __future__ import absolute_import

import os
import os.path
import time

import pymongo

from . import interface
from ... import config
from ... import core
from ... import errors
from ... import utils


class MongoDFixture(interface.Fixture):
    """
    Fixture which provides JSTests with a standalone mongod to run
    against.
    """

    AWAIT_READY_TIMEOUT_SECS = 300

    def __init__(self,
                 logger,
                 job_num,
                 mongod_executable=None,
                 mongod_options=None,
                 dbpath_prefix=None,
                 preserve_dbpath=False):

        interface.Fixture.__init__(self, logger, job_num, dbpath_prefix=dbpath_prefix)

        if "dbpath" in mongod_options and dbpath_prefix is not None:
            raise ValueError("Cannot specify both mongod_options.dbpath and dbpath_prefix")

        # Command line options override the YAML configuration.
        self.mongod_executable = utils.default_if_none(config.MONGOD_EXECUTABLE, mongod_executable)

        self.mongod_options = utils.default_if_none(mongod_options, {}).copy()
        self.preserve_dbpath = preserve_dbpath

        # The dbpath in mongod_options takes precedence over other settings to make it easier for
        # users to specify a dbpath containing data to test against.
        if "dbpath" not in self.mongod_options:
            self.mongod_options["dbpath"] = os.path.join(
                self._dbpath_prefix, config.FIXTURE_SUBDIR)
        self._dbpath = self.mongod_options["dbpath"]

        storage_engine = config.STORAGE_ENGINE
        if "storageEngine" in self.mongod_options:
            storage_engine = self.mongod_options["storageEngine"]
        if storage_engine == "hse":
            self.mongod_options["hseValueCompressionDefault"] = utils.default_if_none(
                config.HSE_COMPR, config.DEFAULT_HSE_COMPR)
            self.mongod_options["hseRestEnabled"] = utils.default_if_none(
                config.HSE_REST_ENABLED, config.DEFAULT_HSE_REST_ENABLED)

        self.mongod = None

    def setup(self):
        """Set up the mongod."""
        if not self.preserve_dbpath and os.path.lexists(self._dbpath):
            utils.rmtree(self._dbpath, ignore_errors=False)

        try:
            os.makedirs(self._dbpath)
        except os.error:
            # Directory already exists.
            pass

        if "port" not in self.mongod_options:
            self.mongod_options["port"] = core.network.PortAllocator.next_fixture_port(self.job_num)
        self.port = self.mongod_options["port"]

        mongod = core.programs.mongod_program(self.logger,
                                              executable=self.mongod_executable,
                                              **self.mongod_options)
        try:
            self.logger.info("Starting mongod on port %d...\n%s", self.port, mongod.as_command())
            mongod.start()
            self.logger.info("mongod started on port %d with pid %d.", self.port, mongod.pid)
        except:
            self.logger.exception("Failed to start mongod on port %d.", self.port)
            raise

        self.mongod = mongod

    def await_ready(self):
        deadline = time.time() + MongoDFixture.AWAIT_READY_TIMEOUT_SECS

        # Wait until the mongod is accepting connections. The retry logic is necessary to support
        # versions of PyMongo <3.0 that immediately raise a ConnectionFailure if a connection cannot
        # be established.
        while True:
            # Check whether the mongod exited for some reason.
            exit_code = self.mongod.poll()
            if exit_code is not None:
                raise errors.ServerFailure("Could not connect to mongod on port %d, process ended"
                                           " unexpectedly with code %d." % (self.port, exit_code))

            try:
                # Use a shorter connection timeout to more closely satisfy the requested deadline.
                client = utils.new_mongo_client(self.port, timeout_millis=500)
                client.admin.command("ping")
                break
            except pymongo.errors.ConnectionFailure:
                remaining = deadline - time.time()
                if remaining <= 0.0:
                    raise errors.ServerFailure(
                        "Failed to connect to mongod on port %d after %d seconds"
                        % (self.port, MongoDFixture.AWAIT_READY_TIMEOUT_SECS))

                self.logger.info("Waiting to connect to mongod on port %d.", self.port)
                time.sleep(0.1)  # Wait a little bit before trying again.

        self.logger.info("Successfully contacted the mongod on port %d.", self.port)

    def _do_teardown(self):
        running_at_start = self.is_running()
        success = True  # Still a success even if nothing is running.

        if not running_at_start and self.port is not None:
            self.logger.info(
                "mongod on port %d was expected to be running in _do_teardown(), but wasn't.",
                self.port)

        if self.mongod is not None:
            if running_at_start:
                self.logger.info("Stopping mongod on port %d with pid %d...",
                                 self.port,
                                 self.mongod.pid)
                self.mongod.stop()

            exit_code = self.mongod.wait()
            success = exit_code == 0

            if running_at_start:
                self.logger.info("Successfully terminated the mongod on port %d, exited with code"
                                 " %d.",
                                 self.port,
                                 exit_code)

        return success

    def is_running(self):
        return self.mongod is not None and self.mongod.poll() is None

    def get_dbpath_prefix(self):
        """ Returns the _dbpath, as this is the root of the data directory. """
        return self._dbpath

    def get_internal_connection_string(self):
        if self.mongod is None:
            raise ValueError("Must call setup() before calling get_internal_connection_string()")

        return "localhost:%d" % self.port

    def get_driver_connection_url(self):
        return "mongodb://" + self.get_internal_connection_string()
