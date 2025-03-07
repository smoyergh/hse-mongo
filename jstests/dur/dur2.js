/**
 *    SPDX-License-Identifier: AGPL-3.0-only
 *
 *    Copyright (C) 2017-2020 Micron Technology, Inc.
 *
 *    This code is derived from and modifies the MongoDB project.
 */

/* test durability
   runs mongod, kill -9's, recovers
*/

var debugging = false;
var testname = "dur2";
var step = 1;
var conn = null;

var start = new Date();
function howLongSecs() {
    return (new Date() - start) / 1000;
}

function log(str) {
    if (str)
        print("\n" + testname + " step " + step++ + " " + str);
    else
        print(testname + " step " + step++);
}

function verify() {
    log("verify");
    var d = conn.getDB("test");
    var mycount = d.foo.count();
    // print("count:" + mycount);

    if (jsTest.options().storageEngine == "hse") {
        d.foo.validate({full: true});
    }

    assert(mycount > 2, "count wrong");
}

function work() {
    log("work");
    x = 'x';
    while (x.length < 1024)
        x += x;
    var d = conn.getDB("test");
    d.foo.drop();
    d.foo.insert({});

    // go long enough we will have time to kill it later during recovery
    var j = 2;
    var MaxTime = 90;
    while (1) {
        d.foo.insert({_id: j, z: x});
        d.foo.update({_id: j}, {$inc: {a: 1}});
        if (j % 25 == 0)
            d.foo.remove({_id: j});
        j++;
        if (j % 3 == 0)
            d.foo.update({_id: j}, {$inc: {a: 1}}, true);
        if (j % 10000 == 0)
            print(j);
        if (howLongSecs() > MaxTime)
            break;
    }

    verify();
    d.runCommand({getLastError: 1, fsync: 1});
}

if (debugging) {
    // mongod already running in debugger
    print(
        "DOING DEBUG MODE BEHAVIOR AS 'db' IS DEFINED -- RUN mongo --nodb FOR REGULAR TEST BEHAVIOR");
    conn = db.getMongo();
    work();
    sleep(30000);
    quit();
}

// directories
var path = MongoRunner.dataPath + testname + "dur";

log("run mongod with --dur");
conn = MongoRunner.runMongod({
    dbpath: path,
    journal: "",
    smallfiles: "",
    journalOptions: 8 /*DurParanoid*/,
    master: "",
    oplogSize: 64
});
work();

log("kill -9");
MongoRunner.stopMongod(conn, /*signal*/ 9);

if (jsTest.options().storageEngine != "hse") {
    // journal file should be present, and non-empty as we killed hard
    assert(listFiles(path + "/journal/").length > 0,
           "journal directory is unexpectantly empty after kill");
}

// restart and recover
log("restart mongod and recover");
conn = MongoRunner.runMongod({
    restart: true,
    cleanData: false,
    dbpath: path,
    journal: "",
    smallfiles: "",
    journalOptions: 8,
    master: "",
    oplogSize: 64
});
verify();

log("stopping mongod " + conn.port);
MongoRunner.stopMongod(conn);

print(testname + " SUCCESS");
