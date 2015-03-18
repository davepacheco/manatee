/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * manateeAdm.test.js: test specific of the "manatee-adm" command.
 * Just run this test directly with Node, not with nodeunit.
 */

var assertplus = require('assert-plus');
var forkexec = require('forkexec');
var fs = require('fs');
var path = require('path');
var vasync = require('vasync');
var VError = require('verror');
var sprintf = require('extsprintf').sprintf;

var testFileName = path.join(
    process.env['TMP'] || process.env['TMPDIR'] || '/var/tmp',
    path.basename(process.argv[1]) + '.' + process.pid);

function MockState() {
    this.pgs_peers = {};
    this.pgs_generation = 3;
    this.pgs_initwal = '3/12345678';
    this.pgs_primary = null;
    this.pgs_sync = null;
    this.pgs_asyncs = [];
    this.pgs_deposed = [];
    this.pgs_frozen = false;
    this.pgs_freeze_reason = null;
    this.pgs_freeze_time = null;
    this.pgs_singleton = false;
    this.pgs_errors = [];
    this.pgs_warnings = [];

    this._npeers = 0;
    this._ip_base = '10.0.0.';

    /*
     * In order to get the same uuids each time we run this program, we hardcode
     * them.
     */
    this._uuids = [
        '301e2d2c-cd09-11e4-837d-13ea7132a060',
        '30219700-cd09-11e4-a971-cf7f957afa2d',
        '3022acc6-cd09-11e4-9716-1ff89e748aff',
        '30250e3a-cd09-11e4-a9e0-cbb241418d62',
        '30265b32-cd09-11e4-b42b-d3e4ff184bb8',
        '3028df9c-cd09-11e4-baa6-23ac5bd2e03c',
        '302b6db6-cd09-11e4-af3b-5bda767e5008',
        '302d2bec-cd09-11e4-a023-a3ebc0c6fd36',
        '302f53b8-cd09-11e4-b38f-a35b1d876bb4',
        '30314268-cd09-11e4-bf7c-4f69c228740e'
    ];
}

MockState.prototype.addPeer = function (role, peerstatus, lag) {
    var ip, zoneId, id, pgUrl, backupUrl, pgerr, repl, lagobj;
    var peer;

    assertplus.ok(this._uuids.length > 0, 'too many peers defined ' +
        '(define more uuids)');
    assertplus.string(role, 'role');
    assertplus.string(peerstatus);
    if (peerstatus == 'down') {
        pgerr = new VError('failed to connect to peer: ECONNREFUSED');
        repl = null;
    } else if (peerstatus == 'no-repl') {
        pgerr = null;
        repl = {};
    } else if (peerstatus == 'sync' || peerstatus == 'async') {
        repl = {
            /* We don't bother populating fields that aren't used here. */
            'state': 'streaming',
            'sync_state': peerstatus,
            'client_addr': null,
            'sent_location': '0/12345678',
            'flush_location': '0/12345678',
            'write_location': '0/12345678',
            'replay_location': '0/12345678'
        };
    } else {
        throw (new VError('unsupported peer status: "%s"', peerstatus));
    }

    if (role == 'async') {
        if (lag === null)
            lag = 342;
        lagobj = { 'minutes': Math.floor(lag / 60), 'seconds': lag % 60 };
    } else {
        lagobj = null;
    }

    ip = this._ip_base + (++this._npeers);
    zoneId = this._uuids.shift();
    id = sprintf('%s:5432:12345-0000000001', ip);
    pgUrl = sprintf('tcp://postgres@%s:5432/postgres', ip);
    backupUrl = sprintf('http://%s:12345', ip);

    /*
     * Construct the new peer details.
     */
    peer = {};
    peer.pgp_label = zoneId.substr(0, 8);
    peer.pgp_ident = {
        'id': id,
        'ip': ip,
        'zoneId': zoneId,
        'pgUrl': pgUrl,
        'backupUrl': backupUrl
    };
    peer.pgp_pgerr = pgerr;
    peer.pgp_lag = lagobj;
    peer.pgp_repl = repl;

    /*
     * Hook this new peer into the simulated cluster state.
     */
    this.pgs_peers[id] = peer;
    switch (role) {
    case 'primary':
        this.pgs_primary = id;
        break;

    case 'sync':
        this.pgs_sync = id;
        break;

    case 'async':
        this.pgs_asyncs.push(id);
        break;

    case 'deposed':
        this.pgs_deposed.push(id);
        break;

    default:
        throw (new VError('unsupported role: "%s"', role));
    }
};

MockState.prototype.finish = function ()
{
    var self = this;
    var peers, i;

    /*
     * Hook up replication connections.
     */
    peers = [ this.pgs_peers[this.pgs_primary] ];
    if (this.pgs_sync !== null) {
        peers.push(this.pgs_peers[this.pgs_sync]);

        this.pgs_asyncs.forEach(function (a) {
            peers.push(self.pgs_peers[a]);
        });
    }

    for (i = 0; i < peers.length - 1; i++) {
        if (peers[i].pgp_repl === null ||
            !peers[i].pgp_repl.hasOwnProperty('client_addr'))
            break;

        peers[i].pgp_repl.client_addr = peers[i + 1].pgp_ident.ip;
    }

    /* XXX use common code to compute warnings and errors */
};

MockState.prototype.toJson = function ()
{
    var obj, prop;

    obj = {};
    for (prop in this) {
        if (!this.hasOwnProperty(prop))
            continue;

        if (prop.substr(0, 'pgs_'.length) != 'pgs_')
            continue;

        obj[prop] = this[prop];
    }

    return (JSON.stringify(obj));
};


/*
 * In order to test various outputs, we generate a bunch of cluster states and
 * trick the loadClusterDetails() interface in lib/adm.js to return these
 * instead of actually contacting any remote servers.
 */
var clusterStates = {
    'singletonOk': makeStateSingleton({}),
    'singletonDown': makeStateSingleton({ 'primary': 'down' }),

    'normalOk': makeStateNormal({}),
    'normal2Peers': makeStateNormal({ 'asyncs': 0 }),
    'normal5Peers': makeStateNormal({ 'asyncs': 3 }),
    'normalDeposed': makeStateNormal({ 'deposed': 1 }),
    'normal2Deposed': makeStateNormal({ 'deposed': 2 }),
    'normal5Peers2deposed': makeStateNormal({
        'asyncs': 3,
        'deposed': 2
    }),
    'normalPdown': makeStateNormal({ 'primary': 'down' }),
    'normalPnorepl': makeStateNormal({ 'primary': 'no-repl' }),
    'normalPasync': makeStateNormal({ 'sync': 'async' }),
    'normalSdown': makeStateNormal({ 'sync': 'down' }),
    'normalSnorepl': makeStateNormal({ 'sync': 'no-repl' }),

    'normalNoLag': makeStateNormal({ 'lag': 0 }),
    'normalLargeLag': makeStateNormal({ 'lag': 86465 })
};

function makeStateSingleton(options)
{
    var state;

    assertplus.object(options);
    assertplus.optionalString(options.primary);

    state = new MockState();
    state.pgs_singleton = true;
    state.addPeer('primary', options.primary || 'no-repl', 'none');
    state.finish();
    return (state);
}

function makeStateNormal(options)
{
    var state, i;
    var nasyncs = 1;
    var ndeposed = 0;

    assertplus.object(options);
    assertplus.optionalString(options.primary);
    assertplus.optionalNumber(options.asyncs);
    assertplus.optionalNumber(options.deposed);

    if (options.hasOwnProperty('asyncs'))
        nasyncs = options.asyncs;
    if (options.hasOwnProperty('deposed'))
        ndeposed = options.deposed;

    state = new MockState();
    state.addPeer('primary', options.primary || 'sync');
    state.addPeer('sync', options.sync ||
        (nasyncs === 0 ? 'no-repl' : 'async'));

    for (i = 0; i < nasyncs; i++) {
        state.addPeer('async', i == nasyncs - 1 ? 'no-repl' : 'async',
            options.hasOwnProperty('lag') ? options.lag : null);
    }

    for (i = 0; i < ndeposed; i++) {
        state.addPeer('deposed', 'no-repl');
    }

    state.finish();
    return (state);
}

function runTestCase(testname, execArgs, callback)
{
    var cs, json, execname;

    cs = clusterStates[testname];
    json = cs.toJson();
    fs.writeFileSync(testFileName, json);
    execname = path.join(__dirname, '..', 'bin', 'manatee-adm');

    forkexec.forkExecWait({
        'argv': [ execname ].concat(execArgs),
        'timeout': 5000,
        'env': {
            'PATH': process.env['PATH'], /* for shebang */
            'SHARD': 'UNUSED',
            'ZK_IPS': 'UNUSED',
            'MANATEE_ADM_TEST_STATE': testFileName
        }
    }, function (err, info) {
        if (err) {
            console.error('manatee-adm exec failed: ', info);
            callback(err);
            return;
        }

        /* XXX actually have catest check against correct values */
        fs.unlink(testFileName, function (warn) {
            if (warn) {
                console.error('warning: failed to unlink "%s": %s',
                    testFileName, err.message);
            }

            console.log('TEST CASE "%s": manatee-adm %s:', testname,
                execArgs.join(' '));
            console.log('-----------------------------');
            process.stdout.write(info.stdout);
            console.log('-----------------------------');
            console.log('');
            if (info.stderr.length > 0) {
                callback(new VError('non-empty stderr: "%s"', info.stderr));
            } else {
                callback();
            }
        });
    });
}

function main()
{
    var testname, funcs;

    funcs = [];
    for (testname in clusterStates) {
        funcs.push(runTestCase.bind(null, testname, [ 'peers' ]));
        funcs.push(runTestCase.bind(null, testname, [ 'pg-status' ]));
        funcs.push(runTestCase.bind(null, testname, [ 'show' ]));
        funcs.push(runTestCase.bind(null, testname, [ 'show', '-v' ]));
        funcs.push(runTestCase.bind(null, testname, [ 'verify' ]));
        funcs.push(runTestCase.bind(null, testname, [ 'verify', '-v' ]));
    }

    /*
     * XXX add test cases for:
     *     -H, -o flags for "peers"
     *     -H, -o flags for "pg-status"
     *     should fail: -o with pg-specific field for "peers"
     *     should fail: -o with invalid field for "peers"
     *     should fail: -o with invalid field for "pg-status"
     */

    console.error('using tmpfile: "%s"', testFileName);
    vasync.waterfall(funcs, function (err) {
        if (err) {
            console.error('error: %s', err.message);
            process.exit(1);
        }

        console.log('TEST PASSED');
    });
}

main();
