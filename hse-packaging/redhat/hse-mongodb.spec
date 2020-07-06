#
#    SPDX-License-Identifier: AGPL-3.0-only
#
#    Copyright (C) 2017-2020 Micron Technology, Inc.
#
#    This code is derived from and modifies the MongoDB project.
#

#
# NOTE: This file is mostly based on upstream except for hse-mongodb-test,
#       which is based on the Fedora project's RPM spec file for MongoDB.
#

Name: hse-mongodb
Prefix: /usr
Conflicts: mongodb, mongodb-org
Version: %{dynamic_version}
Release: %{dynamic_release}%{?dist}
Summary: MongoDB with Heterogeneous-memory Storage Engine (metapackage)
License: AGPL 3.0
URL: https://github.com/hse-project/hse-mongo
Vendor: Micron Technology, Inc.
Group: Applications/Databases
Requires: hse-mongodb-server = %{version}, hse-mongodb-shell = %{version}, hse-mongodb-mongos = %{version}, hse-mongodb-tools = %{version}

Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
MongoDB is a popular document-oriented NoSQL database.  The MongoDB implementation is open source, and includes an extensible framework for integrating different storage engines.

This version includes support for the Heterogeneous-memory Storage Engine (HSE).  HSE is an embeddable key-value store designed for SSDs based on NAND flash or persistent memory.

This metapackage will install the mongo shell, import/export tools, other client utilities, server software, default configuration, and systemd service files.

%package server
Summary: MongoDB database server with Heterogeneous-memory Storage Engine
Group: Applications/Databases
Requires: hse
Requires: mpool
Requires: numactl
Requires: openssl
Conflicts: mongodb-server, mongodb-org-server

%description server
MongoDB is a popular document-oriented NoSQL database.  The MongoDB implementation is open source, and includes an extensible framework for integrating different storage engines.

This version includes support for the Heterogeneous-memory Storage Engine (HSE).  HSE is an embeddable key-value store designed for SSDs based on NAND flash or persistent memory.

This package contains the MongoDB server software, default configuration files, and systemd service files.

%package shell
Summary: MongoDB shell client
Group: Applications/Databases
Requires: openssl
Conflicts: mongodb, mongodb-org-shell

%description shell
This package contains the mongo shell.

%package mongos
Summary: MongoDB sharded cluster query router
Group: Applications/Databases
Conflicts: mongodb-server, mongodb-org-mongos

%description mongos
This package contains mongos, the MongoDB sharded cluster query router.

%package tools
Summary: MongoDB tools
Group: Applications/Databases
Requires: openssl
Conflicts: mongo-tools, mongodb-org-tools

%description tools
This package contains standard utilities for interacting with MongoDB.

%package test
Summary: MongoDB regression test suite
Group: Applications/Databases
Requires: hse-mongodb-server, hse-mongodb-shell, hse-mongodb-mongos, hse-mongodb-tools

%description test
This package contains the regression test suite distributed with the MongoDB sources.

%prep
%setup
# fix distro specific paths
sed -i -r "s|/etc/default|/etc/sysconfig|" conf/mongod.service
sed -i -r "s|/var/lib/mongodb|/var/lib/mongo|" conf/mongod.conf

# by default use system mongod, mongos and mongo binaries in resmoke.py
sed -i -r "s|os.curdir(, \"mongo\")|\"%{_bindir}\"\1|" buildscripts/resmokelib/config.py
sed -i -r "s|os.curdir(, \"mongod\")|\"%{_bindir}\"\1|" buildscripts/resmokelib/config.py
sed -i -r "s|os.curdir(, \"mongos\")|\"%{_bindir}\"\1|" buildscripts/resmokelib/config.py

# set default data prefix in resmoke.py
sed -i -r "s|/data/db|%{_datadir}/%{name}-test/var|" buildscripts/resmokelib/config.py

%debug_package

%build

%install
mkdir -p $RPM_BUILD_ROOT/usr
cp -rv bin $RPM_BUILD_ROOT/usr
# Don't include dbtest in /usr/bin
rm -f $RPM_BUILD_ROOT/usr/bin/dbtest
mkdir -p $RPM_BUILD_ROOT/usr/share/man/man1
cp manpages/*.1 $RPM_BUILD_ROOT/usr/share/man/man1/
# FIXME: remove this rm when mongosniff is back in the package
rm -v $RPM_BUILD_ROOT/usr/share/man/man1/mongosniff.1*
mkdir -p $RPM_BUILD_ROOT/etc
cp -v conf/mongod.conf $RPM_BUILD_ROOT/etc/mongod.conf
mkdir -p $RPM_BUILD_ROOT/etc/sysconfig
cp -v conf/mongod.default $RPM_BUILD_ROOT/etc/sysconfig/mongod
mkdir -p $RPM_BUILD_ROOT/lib/systemd/system
cp -v conf/mongod.service $RPM_BUILD_ROOT/lib/systemd/system
mkdir -p $RPM_BUILD_ROOT/var/lib/mongo
mkdir -p $RPM_BUILD_ROOT/var/log/mongodb
mkdir -p $RPM_BUILD_ROOT/var/run/mongodb
touch $RPM_BUILD_ROOT/var/log/mongodb/mongod.log

#
# TEST PACKAGE
#
mkdir -p $RPM_BUILD_ROOT/usr/share/%{name}-test
mkdir -p $RPM_BUILD_ROOT/usr/share/%{name}-test/buildscripts
mkdir -p $RPM_BUILD_ROOT/usr/share/%{name}-test/var

install -p -D -m 755 bin/dbtest $RPM_BUILD_ROOT/usr/share/%{name}-test/
install -p -D -m 755 buildscripts/resmoke.py $RPM_BUILD_ROOT/usr/share/%{name}-test/
install -p -D -m 444 buildscripts/__init__.py $RPM_BUILD_ROOT/usr/share/%{name}-test/buildscripts/

cp -R buildscripts/resmokeconfig $RPM_BUILD_ROOT/usr/share/%{name}-test/buildscripts/
cp -R buildscripts/resmokelib $RPM_BUILD_ROOT/usr/share/%{name}-test/buildscripts/
cp -R jstests $RPM_BUILD_ROOT/usr/share/%{name}-test/

# Appease a handful of tests that expect the binaries to exist in the top level directory
ln -s /usr/bin/mongo $RPM_BUILD_ROOT/usr/share/%{name}-test/mongo
ln -s /usr/bin/mongod $RPM_BUILD_ROOT/usr/share/%{name}-test/mongod
ln -s /usr/bin/mongos $RPM_BUILD_ROOT/usr/share/%{name}-test/mongos

# Special case files need user-only read access for auth tests to work
chmod 600 $RPM_BUILD_ROOT/usr/share/%{name}-test/jstests/libs/authTestsKey
chmod 600 $RPM_BUILD_ROOT/usr/share/%{name}-test/jstests/libs/key1
chmod 600 $RPM_BUILD_ROOT/usr/share/%{name}-test/jstests/libs/key2

# Remove executable flag from JS tests
for file in `find $RPM_BUILD_ROOT/usr/share/%{name}-test/jstests -type f`; do
    chmod a-x $file
done
#
# END TEST PACKAGE
#

%clean
rm -rf $RPM_BUILD_ROOT

%pre server
if ! /usr/bin/id -g mongod &>/dev/null; then
    /usr/sbin/groupadd -r mongod
fi
if ! /usr/bin/id mongod &>/dev/null; then
    /usr/sbin/useradd -M -r -g mongod -d /var/lib/mongo -s /bin/false   -c mongod mongod > /dev/null 2>&1
fi

%post server
if test $1 = 1
then
  /usr/bin/systemctl daemon-reload
  /usr/bin/systemctl enable mongod
fi

%preun server
if test $1 = 0
then
  /usr/bin/systemctl daemon-reload
  /usr/bin/systemctl disable mongod
fi

%postun server
if test $1 -ge 1
then
  /usr/bin/systemctl daemon-reload
  /usr/bin/systemctl restart mongod >/dev/null 2>&1 || :
fi

%files

%files server
%defattr(-,root,root,-)
%config(noreplace) /etc/mongod.conf
%config(noreplace) /etc/sysconfig/mongod
%{_bindir}/mongod
%{_mandir}/man1/mongod.1
/lib/systemd/system/mongod.service
%attr(0755,mongod,mongod) %dir /var/lib/mongo
%attr(0755,mongod,mongod) %dir /var/log/mongodb
%attr(0755,mongod,mongod) %dir /var/run/mongodb
%attr(0640,mongod,mongod) %config(noreplace) %verify(not md5 size mtime) /var/log/mongodb/mongod.log
%doc GNU-AGPL-3.0
%doc README
%doc THIRD-PARTY-NOTICES
%doc MPL-2



%files shell
%defattr(-,root,root,-)
%{_bindir}/mongo
%{_bindir}/mongobridge
%{_mandir}/man1/mongo.1

%files mongos
%defattr(-,root,root,-)
%{_bindir}/mongos
%{_mandir}/man1/mongos.1

%files tools
%defattr(-,root,root,-)
#%doc README GNU-AGPL-3.0.txt

%{_bindir}/bsondump
%{_bindir}/mongodump
%{_bindir}/mongoexport
%{_bindir}/mongofiles
%{_bindir}/mongoimport
%{_bindir}/mongooplog
%{_bindir}/mongoperf
%{_bindir}/mongorestore
%{_bindir}/mongotop
%{_bindir}/mongostat

%{_mandir}/man1/bsondump.1
%{_mandir}/man1/mongodump.1
%{_mandir}/man1/mongoexport.1
%{_mandir}/man1/mongofiles.1
%{_mandir}/man1/mongoimport.1
%{_mandir}/man1/mongooplog.1
%{_mandir}/man1/mongoperf.1
%{_mandir}/man1/mongorestore.1
%{_mandir}/man1/mongotop.1
%{_mandir}/man1/mongostat.1

%files test
%defattr(-,mongod,root)
%dir %attr(0755, mongod, root) %{_datadir}/%{name}-test
%dir %attr(0755, mongod, root) %{_datadir}/%{name}-test/var
%{_datadir}/%{name}-test/jstests
%{_datadir}/%{name}-test/buildscripts
%{_datadir}/%{name}-test/resmoke.py
%{_datadir}/%{name}-test/dbtest
%{_datadir}/%{name}-test/mongo
%{_datadir}/%{name}-test/mongod
%{_datadir}/%{name}-test/mongos

%changelog
* Fri Jun 12 2020 Tom Blamer <tblamer@micron.com>
- Revamped RPM packaging for hse-mongodb
