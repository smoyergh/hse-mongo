#
#    SPDX-License-Identifier: AGPL-3.0-only
#
#    Copyright (C) 2017-2020 Micron Technology, Inc.
#
#    This code is derived from and modifies the MongoDB project.
#

"""
Enables supports for running tests simultaneously by processing them
from a multi-consumer queue.
"""

from __future__ import absolute_import

import sys

from .. import config
from .. import errors
from .. import logging
from ..utils import queue as _queue


class Job(object):
    """
    Runs tests from a queue.
    """

    def __init__(self, logger, fixture, hooks, report, archival):
        """
        Initializes the job with the specified fixture and custom
        behaviors.
        """

        self.logger = logger
        self.fixture = fixture
        self.hooks = hooks
        self.report = report
        self.archival = archival

    def __call__(self, queue, interrupt_flag, teardown_flag=None):
        """
        Continuously executes tests from 'queue' and records their
        details in 'report'.

        If 'teardown_flag' is not None, then 'self.fixture.teardown()'
        will be called before this method returns. If an error occurs
        while destroying the fixture, then the 'teardown_flag' will be
        set.
        """

        should_stop = False
        try:
            self._run(queue, interrupt_flag)
        except errors.StopExecution as err:
            # Stop running tests immediately.
            self.logger.error("Received a StopExecution exception: %s.", err)
            should_stop = True
        except:
            # Unknown error, stop execution.
            self.logger.exception("Encountered an error during test execution.")
            should_stop = True

        if should_stop:
            # Set the interrupt flag so that other jobs do not start running more tests.
            interrupt_flag.set()
            # Drain the queue to unblock the main thread.
            Job._drain_queue(queue)

        if teardown_flag is not None:
            try:
                if not self.fixture.teardown(finished=True):
                    self.logger.warn("Teardown of %s was not successful.", self.fixture)
                    teardown_flag.set()
            except:
                self.logger.exception("Encountered an error while tearing down %s.", self.fixture)
                teardown_flag.set()

    def _run(self, queue, interrupt_flag):
        """
        Calls the before/after suite hooks and continuously executes
        tests from 'queue'.
        """

        for hook in self.hooks:
            hook.before_suite(self.report)

        while not interrupt_flag.is_set():
            test = queue.get_nowait()
            try:
                if test is None:
                    # Sentinel value received, so exit.
                    break
                self._execute_test(test)
            finally:
                queue.task_done()

        for hook in self.hooks:
            hook.after_suite(self.report)

    def _execute_test(self, test):
        """
        Calls the before/after test hooks and executes 'test'.
        """

        test.configure(self.fixture, config.NUM_CLIENTS_PER_FIXTURE)
        self._run_hooks_before_tests(test)

        test(self.report)
        try:
            if config.FAIL_FAST and not self.report.wasSuccessful():
                self.logger.info("%s failed, so stopping..." % (test.shortDescription()))
                raise errors.StopExecution("%s failed" % (test.shortDescription()))

            if not self.fixture.is_running():
                self.logger.error(
                    "%s marked as a failure because the fixture crashed during the test.",
                    test.shortDescription())
                self.report.setFailure(test, return_code=2)

                if config.CONTINUE_ON_CRASH:
                    self.logger.info("Trying to start the fixture back up again...")

                    # If the fixture owned multiple mongods (i.e. replicaset or
                    # shardedcluster), make sure any orphaned processes are
                    # cleaned up before attempting to start the fixture again.
                    self.fixture.teardown()

                    self.fixture.setup()
                    self.fixture.await_ready()

                if not self.fixture.is_running():
                    raise errors.StopExecution("%s not running after %s" %
                                               (self.fixture, test.shortDescription()))

        finally:
            success = self.report.find_test_info(test).status == "pass"
            if self.archival:
                self.archival.archive(self.logger, test, success)

        self._run_hooks_after_tests(test)

    def _run_hook(self, hook, hook_function, test):
        """Provide helper to run hook and archival."""
        try:
            success = False
            hook_function(test, self.report)
            success = True
        finally:
            if self.archival:
                self.archival.archive(self.logger, test, success, hook=hook)

    def _run_hooks_before_tests(self, test):
        """
        Runs the before_test method on each of the hooks.

        Swallows any TestFailure exceptions if set to continue on
        failure, and reraises any other exceptions.
        """

        try:
            for hook in self.hooks:
                self._run_hook(hook, hook.before_test, test)

        except errors.StopExecution:
            raise

        except errors.ServerFailure:
            self.logger.exception("%s marked as a failure by a hook's before_test.",
                                  test.shortDescription())
            self._fail_test(test, sys.exc_info(), return_code=2)
            raise errors.StopExecution("A hook's before_test failed")

        except errors.TestFailure:
            self.logger.exception("%s marked as a failure by a hook's before_test.",
                                  test.shortDescription())
            self._fail_test(test, sys.exc_info(), return_code=1)
            if config.FAIL_FAST:
                raise errors.StopExecution("A hook's before_test failed")

        except:
            # Record the before_test() error in 'self.report'.
            self.report.startTest(test)
            self.report.addError(test, sys.exc_info())
            self.report.stopTest(test)
            raise

    def _run_hooks_after_tests(self, test):
        """
        Runs the after_test method on each of the hooks.

        Swallows any TestFailure exceptions if set to continue on
        failure, and reraises any other exceptions.
        """
        for hook in self.hooks:
            try:
                hook.after_test(test, self.report)

            except errors.StopExecution:
                raise

            except errors.ServerFailure:
                self.logger.exception("%s marked as a failure by a hook's after_test.",
                                      test.shortDescription())
                self.report.setFailure(test, return_code=2)
                if not config.CONTINUE_ON_CRASH:
                    raise errors.StopExecution("A hook's after_test failed")

            except errors.TestFailure:
                self.logger.exception("%s marked as a failure by a hook's after_test.",
                                      test.shortDescription())
                self.report.setFailure(test, return_code=1)
                if config.FAIL_FAST:
                    raise errors.StopExecution("A hook's after_test failed")

            except:
                self.report.setError(test)
                raise

    def _fail_test(self, test, exc_info, return_code=1):
        """
        Helper to record a test as a failure with the provided return
        code.

        This method should not be used if 'test' has already been
        started, instead use TestReport.setFailure().
        """

        self.report.startTest(test)
        test.return_code = return_code
        self.report.addFailure(test, exc_info)
        self.report.stopTest(test)

    @staticmethod
    def _drain_queue(queue):
        """
        Removes all elements from 'queue' without actually doing
        anything to them. Necessary to unblock the main thread that is
        waiting for 'queue' to be empty.
        """

        try:
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()
        except _queue.Empty:
            # Multiple threads may be draining the queue simultaneously, so just ignore the
            # exception from the race between queue.empty() being false and failing to get an item.
            pass
