/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * @overview Administration library.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var assert = require('assert-plus');
var bignum = require('bignum');
var bunyan = require('bunyan');
var exec = require('child_process').exec;
var once = require('once');
var path = require('path');
var pg = require('pg');
var progbar = require('progbar');
var prompt = require('prompt');
var restify = require('restify');
var sprintf = require('extsprintf').sprintf;
var vasync = require('vasync');
var VError = require('verror');
var zfs = require('./zfsClient');
var zk = require('node-zookeeper-client');

/*
 * Exported interface.  These functions are only exported to the manatee-adm
 * command.  Think very hard before exporting these as a public Manatee client
 * library.
 */
exports.checkLock = checkLock;
exports.freeze = freeze;
exports.unfreeze = unfreeze;
exports.history = history;
exports.rebuild = rebuild;
exports.reap = reap;
exports.setOnwm = setOnwm;
exports.zkActive = zkActive;
exports.zkState = zkState;
exports.stateBackfill = stateBackfill;
exports.status = status;
exports.loadClusterDetails = loadClusterDetails;


// Constants
var PG_REPL_STAT = 'select * from pg_stat_replication;';
var PG_REPL_LAG = 'SELECT now() - pg_last_xact_replay_timestamp() AS time_lag;';
var SPINNER = ['-', '\\', '|', '/'];

var LOG = bunyan.createLogger({
    name: 'manatee-adm',
    level: (process.env.LOG_LEVEL || 'fatal'),
    src: true,
    serializers: {
        err: bunyan.stdSerializers.err
    }
});


/*
 * The various pipeline functions here all assume that the first argument is a
 * common state object with some of the following properties:
 *
 * Properties initialized in the constructor:
 *
 *     zk (string)              ZooKeeper connection string
 *
 *     shard (optional string)  Name of shard to operate on.  If not present,
 *                              then operate on all shards.  Only status()
 *                              supports this.
 *
 *     legacyOrderMode          Fetch cluster details based on v1.0 semantics.
 *
 * Properties filled in by various pipeline functions:
 *
 *     zkClient                 ZooKeeper client
 *
 *     shards                   List of all shard (cluster) names
 *     (array of string)
 *
 *     state                    If "shard" is present, this is a single shard's
 *                              state.  Otherwise, this is all shard states,
 *                              keyed by shard name.
 *
 *     stateStat                If "shard" is present, this is the single
 *                              shard's state ZK metadata.
 *
 *     formattedState           Legacy "status" representation for each shard,
 *                              keyed by shard name.
 *
 *     activeData               Active nodes, keyed by shard name.
 *
 *     fullState                ManateeClusterDetails, see below.
 *
 * Each of the pipeline functions takes a ClusterFetchState argument with some
 * of these properties set and fills in others.
 */
function ClusterFetchState(args)
{
    assert.object(args, 'args');
    assert.string(args.zk, 'args.zk');
    assert.optionalString(args.shard, 'args.shard');
    assert.optionalBool(args.legacyOrderMode, 'args.legacyOrderMode');

    this.zk = args.zk;
    this.shard = args.shard || null;
    this.legacyOrderMode = args.legacyOrderMode || false;

    this.zkClient = null;
    this.shards = null;
    this.state = null;
    this.stateStat = null;
    this.formattedState = null;
    this.activeData = null;
    this.fullState = null;
}

/*
 * See ClusterFetchState above.
 * Expects: "zk"
 * Populates: "zkClient"
 */
function cfsCreateZkClient(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    createZkClient(cfs.zk, function (err, client) {
        cfs.zkClient = client;
        cb(err);
    });
}

/*
 * This one's not actually a pipeline function (and takes no callback).  It's
 * invoked after the pipeline completes, typically on success or failure.
 */
function cfsCloseZkClient(cfs) {
    assert.ok(cfs instanceof ClusterFetchState);
    if (cfs.zkClient) {
        cfs.zkClient.removeAllListeners();
        cfs.zkClient.close();
    }
}

/*
 * See ClusterFetchState above.
 * Expects: "zkClient" and optionally "shard".
 * Populates: "shards".  If "shard" is present, just uses that shard. Otherwise,
 * lists all shards.
 */
function cfsListShards(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    assert.object(cfs.zkClient, 'cfs.zkClient');
    cfs.shards = [];

    if (cfs.shard) {
        cfs.shards.push(cfs.shard);
        setImmediate(cb);
    } else {
        cfs.zkClient.getChildren('/manatee', function (err, s) {
            if (err) {
                cb(err);
            } else {
                cfs.shards = s;
                cb();
            }
        });
    }
}

/*
 * See ClusterFetchState above.  If "legacyOrderMode" is true, then fetches
 * all clusters' states, intepreted using legacy semantics.  Otherwise, fetches
 * all clusters' states interpreted using current semantics.
 */
function cfsFetchMaybeLegacyClusterStates(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    if (cfs.legacyOrderMode) {
        cfsFetchClusterStatesV1(cfs, cb);
    } else {
        cfsFetchClusterStatesV2(cfs, function (err) {
            if (err && err.name === 'NO_NODE') {
                err = new Error('no state object exists ' +
                    'for one or more shards: ' +
                    cfs.shards.join(', '));
            }
            cb(err);
        });
    }
}

/**
 * See ClusterFetchState above.
 * Expects: "zkClient", "shard"
 * Populates: "state", "stateStat"
 */
function cfsFetchClusterState(cfs, cb) {
    var zkpath;

    assert.ok(cfs instanceof ClusterFetchState);
    assert.string(cfs.shard, 'cfs.shard');
    assert.object(cfs.zkClient, 'cfs.zkClient');

    zkpath = sprintf('/manatee/%s/state', cfs.shard);
    cfs.zkClient.getData(zkpath, function (err, data, stat) {
        if (err) {
            cb(err);
        } else {
            cfs.state = JSON.parse(data.toString('utf8'));
            cfs.stateStat = stat;
            cb();
        }
    });
}

/*
 * See ClusterFetchState above.
 * Expects: "zkClient", "shard", "state", "stateStat"
 */
function cfsPutClusterState(cfs, cb) {
    var zkpath, data;

    assert.ok(cfs instanceof ClusterFetchState);
    assert.string(cfs.shard, 'cfs.shard');
    assert.object(cfs.zkClient, 'cfs.zkClient');
    assert.ok(cfs.stateStat !== null);

    zkpath = sprintf('/manatee/%s/state', cfs.shard);
    data = new Buffer(JSON.stringify(cfs.state, null, 0));
    cfs.zkClient.setData(zkpath, data, cfs.stateStat.version, function (err) {
        cb(err);
    });
}

/*
 * See ClusterFetchState above.
 * Expects: "zkClient", "shards"
 * Populates: "state"
 */
function cfsFetchClusterStatesV2(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    assert.object(cfs.zkClient, 'cfs.zkClient');
    assert.arrayOfString(cfs.shards, 'cfs.shards');
    assert.ok(cfs.state === null);

    cfs.state = {};
    vasync.forEachParallel({
        'inputs': cfs.shards,
        'func': function fetchOneClusterState(shard, subcb) {
            var shardpath = sprintf('/manatee/%s/state', shard);
            cfs.zkClient.getData(shardpath, function (err, sbuffer) {
                if (err) {
                    subcb(new VError(err, 'fetching "%s" from zk', shard));
                    return;
                }

                cfs.state[shard] = JSON.parse(
                    new Buffer(sbuffer).toString('utf8'));
                subcb();
            });
        }
    });
}

/**
 * Manatee v1 defined the cluster topology based on the order in which nodes
 * joined ZK.  We refer to this as "legacy mode".  This function reads the
 * election path and presents that as if it were the correct topology even
 * though the cluster state is the authoritative topology.
 *
 * See the cfsFetchClusterStatesV2 function for what the return object looks
 * like.
 *
 * Expects: "shards", "zkClient"
 * Populates: "state", "children"
 */
function cfsFetchClusterStatesV1(cfs, cb) {
    var children;

    function compareNodeNames(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }

    assert.ok(cfs instanceof ClusterFetchState);
    vasync.pipeline({ 'funcs': [
        function getShardChildren(_, subcb) {
            children = {};
            subcb = once(subcb);
            var barrier = vasync.barrier();

            barrier.on('drain', subcb);

            cfs.shards.forEach(function (shard) {
                barrier.start(shard);
                // shard is just the implicit znode name, so we have to
                // prepend the path prefix.
                var p = '/manatee/' + shard + '/election';
                cfs.zkClient.getChildren(p, function (err1, ch) {
                    if (err1) {
                        return subcb(err1);
                    }
                    ch.sort(compareNodeNames);
                    children[shard] = ch;
                    barrier.done(shard);
                });
            });
        },
        function getPeerState(_, subcb) {
            cfs.state = {};
            subcb = once(subcb);
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return subcb();
            });
            Object.keys(children).forEach(function (shard) {
                if (!cfs.state[shard]) {
                    cfs.state[shard] = {};
                }
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                children[shard].forEach(function (peer, i) {
                    var p = '/manatee/' + shard + '/election/' + peer;
                    var peerName;
                    var pos = 0;
                    switch (i) {
                        case 0:
                            peerName = 'primary';
                            break;
                        case 1:
                            peerName = 'sync';
                            break;
                        default:
                            peerName = 'async';
                            pos = i - 2;
                            break;
                    }
                    barrier.start(shard + peerName + pos);
                    cfs.zkClient.getData(p, function (err, data) {
                        if (err) {
                            return subcb(err);
                        }
                        data = JSON.parse(data.toString());
                        //Since the old structures don't contain a backupUrl,
                        // add that if one doesn't exist.
                        var bu = transformBackupUrl(peer);
                        data.backupUrl = data.backupUrl ? data.backupUrl : bu;

                        //Also add the id.
                        data.id = peer.substring(0, peer.lastIndexOf('-'));

                        if (['primary', 'sync'].indexOf(peerName) !== -1) {
                            cfs.state[shard][peerName] = data;
                        } else {
                            if (!cfs.state[shard][peerName]) {
                                cfs.state[shard][peerName] = [];
                            }
                            cfs.state[shard][peerName][pos] = data;
                        }
                        barrier.done(shard + peerName + pos);
                    });
                });
                barrier.done(shard);
            });
        },
        function getError(_, subcb) {
            subcb = once(subcb);
            var barrier = vasync.barrier();
            barrier.on('drain', function () {
                return subcb();
            });
            cfs.shards.forEach(function (shard) {
                // in case the shard is empty, we set a barrier so we exit.
                barrier.start(shard);
                var p = '/manatee/' + shard + '/error';
                cfs.zkClient.getData(p, function (err, data) {
                    if (err && err.code !== zk.Exception.NO_NODE) {
                        return subcb(err);
                    }
                    if (data) {
                        cfs.state[shard].error = JSON.parse(
                            data.toString('utf8'));
                    }
                    barrier.done(shard);
                });
            });
        }
    ], 'arg': cfs }, cb);
}


/**
 * Adds postgres state to cluster state objects.
 *
 * @param {string} opts.state A cluster state object (see above).
 * @param {function} cb
 *
 * @return {object} opts.state See cfsFetchClusterStatesV2
 */
function cfsFetchPostgresStatus(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    cb = once(cb);
    var barrier = vasync.barrier();
    barrier.on('drain', function () {
        return cb();
    });
    Object.keys(cfs.state).forEach(function (shard) {
        // in case the shard is empty, we set a barrier so we exit.
        barrier.start(shard);
        var peers = [];
        var roles = [];
        if (cfs.state[shard]['primary']) {
            peers.push(cfs.state[shard]['primary']);
            roles.push('primary');
        }
        if (cfs.state[shard]['sync']) {
            peers.push(cfs.state[shard]['sync']);
            roles.push('sync');
        }
        if (cfs.state[shard]['async']) {
            peers = peers.concat(cfs.state[shard]['async']);
            //Janky...
            roles = roles.concat(cfs.state[shard]['async'].map(function () {
                return ('async');
            }));
        }
        peers.forEach(function (entry, i) {
            var pgUrl = entry.pgUrl;
            var peer = roles[i];
            barrier.start(pgUrl);
            queryPg(pgUrl, PG_REPL_STAT, function (err, res) {
                if (err) {
                    entry.error = JSON.stringify(err);
                    entry.online = false;
                    barrier.done(pgUrl);
                } else {
                    entry.online = true;
                    entry.repl = res.rows[0] ? res.rows[0] : {};
                    if (peer !== 'primary' && peer !== 'sync') {
                        queryPg(pgUrl, PG_REPL_LAG,
                                function (err2, res2) {
                                    if (err2) {
                                        entry.error = JSON.stringify(err2);
                                    } else {
                                        entry.lag = res2.rows[0] ?
                                            res2.rows[0] : {};

                                        /*
                                         * The time lag is a time duration value
                                         * in postgres.  node-postgres returns
                                         * these values as an object with
                                         * properties for seconds, minutes, and
                                         * so on, but any zero properties are
                                         * elided.  As a result, an empty object
                                         * denotes a zero interval.  This is
                                         * confusing for operators, so we always
                                         * fill in minutes and seconds with
                                         * explicit zeros if they're not
                                         * present.
                                         */
                                        if (entry.lag.time_lag) {
                                            if (!entry.lag.time_lag.minutes)
                                                entry.lag.time_lag.minutes = 0;
                                            if (!entry.lag.time_lag.seconds)
                                                entry.lag.time_lag.seconds = 0;
                                        }
                                    }

                                    barrier.done(pgUrl);
                                });
                    } else {
                        barrier.done(pgUrl);
                    }

                }
            });
        });
        barrier.done(shard);
    });
}


/**
 * Formats the state object for a legacy display, unrolling the async array into
 * top-level members of the map.  Primary and sync stay the same, and each of
 * the asyncs after the first have a number appended, in order.
 *
 * Expects: "state"
 * Populates: "formattedState"
 */
function cfsFormatState(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    cfs.formattedState = {};
    Object.keys(cfs.state).forEach(function (shard) {
        cfs.formattedState[shard] = {};
        if (cfs.state[shard].freeze) {
            var f = cfs.state[shard].freeze;
            cfs.formattedState[shard]['__FROZEN__'] = f.date + ': ' +
                f.reason;
        }
        if (cfs.state[shard].primary) {
            cfs.formattedState[shard].primary = cfs.state[shard].primary;
        }
        if (cfs.state[shard].sync) {
            cfs.formattedState[shard].sync = cfs.state[shard].sync;
        }
        if (cfs.state[shard].async) {
            cfs.state[shard].async.forEach(function (e, i) {
                cfs.formattedState[shard]['async' + (i === 0 ? '' : i)] = e;
            });
        }
        if (cfs.state[shard].deposed) {
            cfs.state[shard].deposed.forEach(function (e, i) {
                cfs.formattedState[shard]['deposed' + (i === 0 ? '' : i)] = e;
            });
        }
    });
    setImmediate(cb());
}


/**
 * Expects: "zkClient", "shard"
 * Populates: "activeData"
 */
function cfsActive(cfs, cb) {
    var children, p;

    assert.ok(cfs instanceof ClusterFetchState);
    p = '/manatee/' + cfs.shard + '/election';
    vasync.pipeline({ funcs: [
        function listChildren(_, subcb) {
            cfs.zkClient.getChildren(p, function (err, ch) {
                if (err) {
                    return (subcb(err));
                }
                children = ch;
                return (subcb());
            });
        },
        function fetchChildren(_, subcb) {
            vasync.forEachParallel({
                'inputs': children,
                'func': function (c, subcb2) {
                    var pt = p + '/' + c;
                    cfs.zkClient.getData(pt, function (err, d) {
                        if (err) {
                            return (subcb2(err));
                        }
                        return (subcb2(null, JSON.parse(d.toString('utf8'))));
                    });
                }
            }, function (err, res) {
                if (err) {
                    return (subcb(err));
                }
                cfs.activeData = {};
                res.operations.forEach(function (o, i) {
                    cfs.activeData[children[i]] = o.result;
                });
                return (subcb());
            });
        }
    ], arg: cfs}, function (err) {
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + cfs.shard);
        }
        return (cb(err));
    });
}

/**
 * Returns an object summarizing the complete state of this Manatee cluster
 * (shard).  Logically, this is a JavaScript representation that combines both
 * the cluster state stored in ZooKeeper and the Postgres state of each peer.
 * This can be used to drive the "status" and "pg-status" commands.
 *
 * The return value passed to "callback" is an object with:
 *
 *     pgs_peers        object mapping peer ids (strings) to objects with
 *                      details for each peer.  These objects contain:
 *
 *          pgp_label   display name for this peer
 *
 *          pgp_ident   "peer identifier" object described in the
 *                      manatee-state-machine repo.  The "id" field of this
 *                      object is used as the string peer identifier elsewhere
 *                      in this structure.
 *
 *          pgp_pgerr   if present, describes an error connecting to postgres on
 *                      this peer, which indicates that the rest of the status
 *                      is incomplete
 *
 *          pgp_repl    describes a postgres replication connection for which
 *                      this peer is the upstream.  This object will contain
 *                      fields from postgres's "pg_stat_replication" view.
 *
 *          pgp_lag     if this peer is the target of async replication,
 *                      indicates how long since data was received from
 *                      upstream.  Note that if no writes are being committed
 *                      upstream, this may grow large, and that's not
 *                      necessarily a problem.
 *
 *     pgs_primary      peer id (string) of primary peer
 *
 *     pgs_sync         peer id (string) of sync peer, if any.
 *                      may be null in singleton (one-node-write) mode
 *
 *     pgs_asyncs[]     array of peer ids of async peers
 *
 *     pgs_deposed[]    array of peer ids of deposed peers
 *
 *     pgs_frozen       boolean indicating whether the cluster is frozen
 *
 *     pgs_freeze_reason    if pgs_frozen, then this is a string describing why
 *                          the cluster is frozen
 *
 *     pgs_freeze_time      if pgs_frozen, then this is an ISO 8601 timestamp
 *                          describing when the cluster was frozen
 *
 *     pgs_singleton    whether the cluster is in singleton ("one-node-write")
 *                      mode
 *
 *     pgs_errors[]     array of Errors describing problems with the overall
 *                      health of the cluster that likely mean service is down
 *
 *     pgs_warnings[]   array of Errors describing problems with the cluster
 *                      that likely do not currently affect server.
 *
 * IMPLEMENTATION NOTE: See the IMPORTANT NOTE in the ManateeClusterDetails
 * constructor before making any changes to these fields.
 *
 * Arguments include:
 *
 * @param {Object} args The options object.
 * @param {Object} args.legacyOrderMode fetch state based v1.0 semantics.
 * @param {String} args.zk The zookeeper connection string.
 * @param {String} args.shard The manatee shard name.
 */
function loadClusterDetails(args, cb)
{
    var cfs;

    assert.object(args, 'args');
    assert.optionalBool(args.legacyOrderMode, 'args.legacyOrderMode');
    assert.string(args.shard, 'args.shard');
    assert.string(args.zk, 'args.zk');
    assert.func(cb, 'cb');

    cfs = new ClusterFetchState({
        'legacyOrderMode': args.legacyOrderMode,
        'shard': args.shard,
        'zk': args.zk
    });

    vasync.pipeline({
        'arg': cfs,
        'funcs': [
            cfsCreateZkClient,
            cfsListShards,
            cfsFetchMaybeLegacyClusterStates,
            cfsFetchPostgresStatus,
            cfsConstructClusterDetails
        ]
    }, function (err, results) {
        cfsCloseZkClient(cfs);

        if (err) {
            cb(err);
            return;
        }

        cb(null, cfs.fullState);
    });
}

function cfsConstructClusterDetails(cfs, cb) {
    assert.ok(cfs instanceof ClusterFetchState);
    cfs.fullState = new ManateeClusterDetails(cfs);
    setImmediate(cb);
}

/*
 * This object represents an interface that will hopefully eventually become
 * public.  It's described under loadClusterDetails() above.
 * XXX it would be nice if the various functions in this file populated their
 * data from this structure rather than filling in this structure based on their
 * results.
 */
function ManateeClusterDetails(cfs)
{
    var self = this;
    var st;

    assert.object(cfs, 'cfs');
    assert.string(cfs.shard, 'cfs.shard');
    assert.arrayOfString(cfs.shards, 'cfs.shards');
    /*
     * We would have bailed out already if no shard was found, and the calling
     * interface doesn't support multiple shards.
     */
    assert.ok(cfs.shards.length == 1, 'exactly one shard');
    assert.object(cfs.state, 'cfs.state');
    assert.object(cfs.state[cfs.shard], 'shard state');

    /*
     * IMPORTANT NOTE: This class represents an interface between the Manatee
     * administrative client library and consumers (e.g., the "manatee-adm"
     * command).  This interface is not yet public or committed, but the JSON
     * representation is.
     *
     * Do NOT add, remove, or modify any fields in this class without updating
     * the corresponding documentation for loadClusterDetails() above and
     * without updating the JSON representation.  When making changes, make sure
     * that this object remains immutable and be sure to support strict
     * JavaScript conventions: fields should either be present or "null" (not
     * "undefined" or otherwise falsey).  Every field should have a manatee-adm
     * consumer.
     */
    st = cfs.state[cfs.shard];
    this.pgs_peers = {};
    this.pgs_primary = st.primary.id;
    this.pgs_sync = st.sync === null ? null : st.sync.id;
    this.pgs_asyncs = st.async.map(function (p) { return (p.id); });
    this.pgs_deposed = st.deposed.map(function (p) { return (p.id); });
    this.pgs_generation = st.generation;
    this.pgs_singleton = st.oneNodeWriteMode ? true : false;
    this.pgs_frozen = st.freeze ? true : false;

    if (this.pgs_frozen) {
        if (typeof (st.freeze) == 'object') {
            this.pgs_freeze_reason = st.freeze.reason;
            this.pgs_freeze_time = st.freeze.date;
        } else {
            this.pgs_freeze_reason = 'unknown';
            this.pgs_freeze_time = 'unknown';
        }
    } else {
        this.pgs_freeze_reason = null;
        this.pgs_freeze_time = null;
    }

    this.loadPeer(st.primary);
    if (this.pgs_sync !== null)
        this.loadPeer(st.sync);
    st.async.forEach(function (p) { self.loadPeer(p); });
    st.deposed.forEach(function (p) { self.loadPeer(p); });

    this.pgs_errors = [];
    this.pgs_warnings = [];
    this.loadErrors();
}

/*
 * Given one of the peer objects from the status output, populate an entry in
 * this.pgs_peers.
 */
ManateeClusterDetails.prototype.loadPeer = function (peerinfo)
{
    var identkeys = [ 'id', 'ip', 'pgUrl', 'zoneId', 'backupUrl' ];
    var peer = {};
    var parsed, err;

    peer.pgp_ident = {};
    identkeys.forEach(function (k) {
        assert.string(peerinfo[k], 'expected string for property ' + k);
        peer.pgp_ident[k] = peerinfo[k];
    });
    peer.pgp_label = peerinfo.zoneId.substr(0, 8);
    peer.pgp_repl = peerinfo.repl && peerinfo.repl.state ?
        peerinfo.repl : null;
    peer.pgp_lag = peerinfo.lag && peerinfo.lag.time_lag ?
        peerinfo.lag.time_lag : null;

    if (peerinfo.error) {
        if (typeof (peerinfo.error) == 'string') {
            parsed = JSON.parse(peerinfo.error);
            if (parsed.severity && parsed.file) {
                /* postgres error */
                err = new VError('postgres: ', peerinfo.error);
            } else if (parsed.message) {
                /* Friendly Node error */
                err = new VError('%s', parsed.message);
            } else if (parsed.errno) {
                /* Less friendly Node error */
                err = new VError('%s', parsed.errno);
            } else {
                /* Ugh. */
                err = new VError(peerinfo.error);
            }
        } else {
            err = new Error(peerinfo.error.toString());
        }

        peer.pgp_pgerr = new VError(err, 'peer "%s"', peer.pgp_label);
    } else {
        peer.pgp_pgerr = null;
    }

    this.pgs_peers[peerinfo.id] = peer;
};

ManateeClusterDetails.prototype.loadErrors = function ()
{
    var self = this;
    var p, s;

    p = this.pgs_peers[this.pgs_primary];
    if (p.pgp_pgerr) {
        this.pgs_errors.push(new VError(p.pgp_pgerr,
            'cannot query postgres on primary'));
    }

    if (this.pgs_singleton)
        return;

    s = this.pgs_peers[this.pgs_sync];
    if (s.pgp_pgerr) {
        this.pgs_errors.push(new VError(s.pgp_pgerr,
            'cannot query postgres on sync'));
    }

    if (this.pgs_deposed.length > 0)
        this.pgs_warnings.push(new VError('cluster has a deposed peer'));

    if (this.pgs_asyncs.length === 0)
        this.pgs_warnings.push(new VError('cluster has no async peers'));

    /*
     * If the sync is down, that's all we can really check for now.
     */
    if (s.pgp_pgerr)
        return;

    this.loadReplErrors(p, this.pgs_sync, 'sync', this.pgs_errors);
    this.loadReplErrors(s,
        this.pgs_asyncs.length === 0 ? null : this.pgs_asyncs[0],
        'async', this.pgs_warnings);
    this.pgs_asyncs.forEach(function (a, i) {
        var peer, next;

        if (i < self.pgs_asyncs.length - 1)
            next = self.pgs_asyncs[i + 1];
        else
            next = null;

        peer = self.pgs_peers[a];
        self.loadReplErrors(peer, next, 'async', self.pgs_warnings);
    });
};

ManateeClusterDetails.prototype.loadReplErrors = function (peer, dspeerid,
    kind, errors)
{
    var preverrcount, dspeer;

    assert.object(peer, 'peer');
    assert.string(kind, 'kind');
    preverrcount = errors.length;

    if (dspeerid === null)
        return;

    assert.string(dspeerid, 'dspeerid');
    dspeer = this.pgs_peers[dspeerid];

    /*
     * If there's no replication state, bail out quickly and stop checking for
     * other problems.
     */
    if (peer.pgp_repl === null) {
        errors.push(new VError('peer "%s": downstream replication ' +
            'peer not connected', peer.pgp_label));
        return;
    }

    /*
     * Check the downstream peer IP and the replication state, which is
     * typically "catchup" or "streaming".  We intentionally do both of these
     * checks before returning to report as many relevant, orthogonal issues as
     * we can.
     */
    if (peer.pgp_repl.client_addr != dspeer.pgp_ident.ip) {
        errors.push(new VError('peer "%s": expected downstream peer ' +
            'to be "%s", but found "%s"', peer.pgp_label,
            dspeer.pgp_ident.ip, peer.pgp_repl.client_addr));
    }

    if (peer.pgp_repl.state != 'streaming') {
        errors.push(new VError('peer "%s": downstream replication not ' +
           'yet established (expected state "streaming", found "%s")',
           peer.pgp_label, peer.pgp_repl.state));
    }

    /*
     * Don't check anything else if streaming replication hasn't been
     * established yet.
     */
    if (errors.length > preverrcount)
        return;

    if (peer.pgp_repl.sync_state != kind) {
        errors.push(new VError('peer "%s": expected downstream replication ' +
            'to be "%s", but found "%s"', peer.pgp_label, kind,
            peer.pgp_repl.sync_state));
    }
};

// Operations

/**
 * Returns a deprecated JSON-format summary of the cluster's status.
 *
 * @param {Object} opts The options object.
 * @param {Object} opts.legacyOrderMode Get state based on order of nodes in zk.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} [opts.shard] The manatee shard.
 */
function status(opts, cb) {
    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsListShards,
        cfsFetchMaybeLegacyClusterStates,
        cfsFetchPostgresStatus,
        cfsFormatState
    ], arg: opts}, function (err, results) {
        cfsCloseZkClient(opts);
        return cb(err, opts.formattedState);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function zkState(opts, cb) {
    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsFetchClusterState
    ], arg: opts}, function (err, res) {
        cfsCloseZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.state);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function freeze(opts, cb) {
    assert.string(opts.reason, 'opts.reason');

    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsFetchClusterState,
        function cfsFreeze(cfs, subcb) {
            assert.ok(cfs instanceof ClusterFetchState);
            if (cfs.state.freeze) {
                return (subcb(new VError('shard is already been frozen: "%s"',
                    cfs.state.freeze.reason)));
            }
            cfs.state.freeze = {
                'date': new Date().toISOString(),
                'reason': opts.reason
            };
            return (subcb());
        },
        cfsPutClusterState
    ], arg: opts}, function (err, res) {
        cfsCloseZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function unfreeze(opts, cb) {
    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsFetchClusterState,
        function cfsUnfreeze(cfs, subcb) {
            assert.ok(cfs instanceof ClusterFetchState);
            if (!cfs.state.freeze) {
                return (subcb(new Error('shard is not frozen')));
            }
            delete cfs.state.freeze;
            return (subcb());
        },
        cfsPutClusterState
    ], arg: opts}, function (err, res) {
        cfsCloseZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 * @param {String} opts.zonename The zonename of the node to undepose.
 * @param {String} opts.ip The ip of the node to undepose.
 */
function reap(opts, cb) {
    var cfs;

    cfs = new ClusterFetchState({
        'zk': opts.zk,
        'shard': opts.shard
    });

    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsFetchClusterState,
        function cfsReap(_, subcb) {
            var index = -1;
            cfs.state.deposed.forEach(function (d, i) {
                if ((opts.zonename && opts.zonename === d.zoneId) ||
                    (opts.ip && opts.ip === d.ip)) {
                    index = i;
                }
            });
            if (index === -1) {
                var id = opts.zonename || opts.ip;
                return (subcb(new Error(id + ' not in deposed or does not ' +
                                      'exist')));
            }
            if (index != -1) {
                cfs.state.deposed.splice(index, 1);
            }
            return (subcb());
        },
        cfsPutClusterState
    ], arg: cfs }, function (err, res) {
        cfsCloseZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Boolean} opts.mode True for enabled, false for disabled
 * @param {Boolean} opts.ignorePrompts Ignores prompts if set.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function setOnwm(opts, cb) {
    var cfs;

    cfs = new ClusterFetchState({
        'zk': opts.zk,
        'shard': opts.shard
    });

    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsFetchClusterState,
        function validate(_, subcb) {
            if (opts.mode === 'on' && cfs.state.oneNodeWriteMode === true) {
                return (subcb(new Error(
                    'One node write mode already enabled')));
            }
            if (opts.mode === 'off' &&
                cfs.state.oneNodeWriteMode === undefined) {
                return (subcb(new Error(
                    'One node write mode already disabled')));
            }
            return (subcb());
        },
        function confirm(_, subcb) {
            if (opts.ignorePrompts) {
                return (subcb());
            }
            console.error([
                '!!! WARNING !!!',
                'Enabling or disable one node write mode requires cluster',
                'downtime.  One node write mode in your configuration must',
                'match what is set in the cluster state object in zookeeper.',
                'Please be very careful when enabling or disabling one node',
                'write mode.',
                '!!! WARNING !!!'
            ].join('\n'));
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return subcb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborted...');
                    return subcb(new VError('aborting cluster state ' +
                                          'backfill due to user command'));
                }
                return (subcb());
            });
        },
        function doSetOnwm(_, subcb) {
            if (opts.mode === 'on') {
                cfs.state.oneNodeWriteMode = true;
            } else {
                delete cfs.state.oneNodeWriteMode;
            }
            return (subcb());
        },
        cfsPutClusterState
    ], arg: opts}, function (err, res) {
        cfsCloseZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function zkActive(opts, cb) {
    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsActive
    ], arg: opts}, function (err, res) {
        cfsCloseZkClient(opts);
        if (err && err.name === 'NO_NODE') {
            err = new Error('No state exists for shard ' + opts.shard);
        }
        return cb(err, opts.activeData);
    });
}


/**
 * @param {Object} opts The options object.
 * @param {Boolean} opts.ignorePrompts Ignores prompts if set.
 * @param {Object} opts.zk The zk client config.
 * @param {String} opts.shard The name of the shard.
 */
function stateBackfill(opts, cb) {
    var shardPath = '/manatee/' + opts.shard + '/state';
    var historyPath = '/manatee/' + opts.shard + '/history/0-';
    var cfs = new ClusterFetchState({
        'zk': opts.zk,
        'shard': opts.shard
    });
    opts.noPostgres = true;
    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        function verifyNoState(_, subcb) {
            cfs.zkClient.getData(shardPath, function (err, sbuffer) {
                if (err && err.name === 'NO_NODE') {
                    return (subcb());
                }
                if (err) {
                    return (subcb(err));
                }
                return (subcb(new Error('State already exists for shard ' +
                                      opts.shard)));
            });
        },
        cfsListShards,
        cfsFetchClusterStatesV1,
        function rearrangeState(_, subcb) {
            // Notice we're going to the inner object here...
            var stat = cfs.state[opts.shard];
            //Shift it all by one.
            if (stat.sync && stat.async && stat.async.length >= 1) {
                var newSync = stat.async.pop();
                stat.async.push(stat.sync);
                stat.sync = newSync;
            }
            if (!stat.sync) {
                stat.sync = null;
            }
            if (!stat.async) {
                stat.async = [];
            }
            stat.generation = 0;
            stat.initWal = '0/0000000';
            stat.freeze = {
                'date': new Date().toISOString(),
                'reason': 'manatee-adm state-backfill'
            };
            cfs.newState = stat;
            return (subcb());
        },
        function confirm(_, subcb) {
            console.error('Computed new cluster state:');
            console.error(cfs.newState);
            if (opts.ignorePrompts) {
                return (subcb());
            }
            console.error('is this correct(y/n)');
            prompt.get(['yes'], function (err, result) {
                if (err) {
                    return subcb(err);
                }

                if (result.yes !== 'yes' && result.yes !== 'y') {
                    console.error('aborted...');
                    return subcb(new VError('aborting cluster state ' +
                                          'backfill due to user command'));
                }
                return (subcb());
            });
        },
        function writeState(_, subcb) {
            var hdata = new Buffer(JSON.stringify(cfs.newState));
            var data = new Buffer(JSON.stringify(cfs.newState));
            cfs.zkClient.transaction().
                create(historyPath, hdata,
                       zk.CreateMode.PERSISTENT_SEQUENTIAL).
                create(shardPath, data,
                       zk.CreateMode.PERSISTENT).
                commit(subcb);
        }
    ], arg: opts}, function (err, res) {
        cfsCloseZkClient(cfs);
        return cb(err, cfs.newState);
    });
}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.config The manatee sitter config.
 * @param {Boolean} opts.ignorePrompts Ignores prompts if set.
 */
function rebuild(opts, cb) {
    var cfs;

    cfs = new ClusterFetchState({
        'zk': opts.zk
    });

    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        cfsListShards,
        function findShard(_, subcb) {
            cfs.shard = path.basename(opts.config.shardPath);
            if (cfs.shards.indexOf(cfs.shard) === -1) {
                return (subcb(new Error('unable to determine shards')));
            }
            return (subcb());
        },
        cfsFetchClusterState,
        function checkPrimary(_, subcb) {
            if (!cfs.state.primary) {
                return (subcb(new Error('no primary')));
            }
            return (subcb());
        },
        function checkNotPrimary(_, subcb) {
            if (cfs.state.primary.zoneId === opts.config.zoneId) {
                return (subcb(new Error('This node is the primary.  ' +
                                      'Will not rebuild.')));
            }
            return (subcb());
        },
        //Verify there isn't a zk node for this, otherwise, the primary won't
        // add it once the sitter is restarted.
        cfsActive,
        function checkActive(_, subcb) {
            var act = null;
            Object.keys(cfs.activeData).forEach(function (k) {
                var n = cfs.activeData[k];
                if (n.zoneId === opts.config.zoneId) {
                    act = n;
                }
            });
            if (act) {
                var sec = 60;
                if (opts.config.zkCfg.opts &&
                    opts.config.zkCfg.opts.sessionTimeout) {
                    sec = opts.config.zkCfg.opts.sessionTimeout / 1000;
                }
                return (subcb(new Error('There is an active ZK node for this ' +
                                      'host.  Please disable the sitter and ' +
                                      'let the ZK node timeout (may take up ' +
                                      'to ' + sec + ' seconds).')));
            }
            return (subcb());
        },
        function checkDeposed(_, subcb) {
            opts.removeFromDeposed = false;
            if (!cfs.state.deposed) {
                return (subcb());
            }
            var index = -1;
            cfs.state.deposed.forEach(function (n, i) {
                if (n.zoneId === opts.config.zoneId) {
                    index = i;
                }
            });
            if (index !== -1) {
                opts.deposedIndex = index;
                opts.removeFromDeposed = true;
            }
            return (subcb());
        },
        function promptConfirm(_, subcb) {
            if (opts.ignorePrompts) {
                return (subcb());
            }
            var msg = [
                'This operation will remove all local data and rebuild this',
                'peer from another cluster peer.  This operation is',
                'destructive, and you should only proceed after confirming',
                'that this peer\'s copy of the data is no longer required. ',
                '(yes/no)?'
            ].join(' ');
            var deposedMsg = [
                'This peer is a deposed former primary and may have state',
                'that needs to be removed before it can successfully rejoin',
                'the cluster.  This operation will remove all local data',
                'and then unmark this peer as deposed so that it can rejoin',
                'the cluster.  This operation is destructive, and you should',
                'only proceed after confirming that this peer\'s copy of',
                'the data is no longer required. (yes/no)?'
            ].join(' ');

            console.error(opts.removeFromDeposed ? deposedMsg : msg);
            prompt.get(['no'], function (err, result) {
                if (err) {
                    return subcb(err);
                }

                if (result.no !== 'yes' && result.no !== 'y') {
                    console.error('aborting rollback');
                    return subcb(new VError('aborting rollback ' +
                                          'due to user command'));
                }
                return subcb();
            });
        },
        function deleteDataDir(_, subcb) {
            console.error('removing zfs dataset');
            var cmd = 'rm -rf ' + opts.config.postgresMgrCfg.dataDir + '/*';
            exec(cmd, subcb);
        },
        function removeFromDeposed(_, subcb) {
            if (!opts.removeFromDeposed) {
                return (subcb());
            }
            cfs.state.deposed.splice(opts.deposedIndex, 1);
            cfsPutClusterState(cfs, subcb);
        },
        //This just lets the node recover "naturally"
        function restartSitter(_, subcb) {
            console.error('enabling sitter');
            exec('svcadm enable manatee-sitter', subcb);
        },
        function checkZfsRecv(_, subcb) {
            subcb = once(subcb);
            var client = restify.createJsonClient({
                url: 'http://' + opts.config.ip + ':' +
                    (opts.config.postgresPort + 1),
                version: '*'
            });

            var bar;
            var lastByte = 0;
            var waitCount = 0;

            process.stderr.write('Waiting for zfs recv  ');
            function checkZfsStatus() {
                client.get('/restore', function (err, req, res, obj) {
                    if (err) {
                        LOG.warn({err: err}, 'unable to query zfs status');
                        // give the sitter 30s to start
                        if (++waitCount > 30) {
                            client.close();
                            return subcb(err, 'unable to query zfs status');
                        }
                        setTimeout(checkZfsStatus, 1000);
                        return;
                    } else if (obj.restore && obj.restore.size) {
                        if (obj.restore.done) {
                            LOG.info('zfs receive is done');
                            process.stderr.write('\n');
                            client.close();
                            return subcb();
                        }
                        if (!bar) {
                            bar = new progbar.ProgressBar({
                                filename: obj.restore.dataset,
                                size: parseInt(obj.restore.size, 10)
                            });
                        }
                        if (obj.restore.completed) {
                            var completed = parseInt(obj.restore.completed, 10);
                            var advance = completed - lastByte;
                            lastByte = completed;
                            bar.advance(advance);
                        }
                        setTimeout(checkZfsStatus, 1000);
                        return;
                    } else {
                        process.stderr.write('\b' + SPINNER[waitCount++ %
                                             SPINNER.length]);
                        setTimeout(checkZfsStatus, 1000);
                        return;
                    }
                });
            }
            checkZfsStatus();
        }
    ], arg: opts}, function (err, results) {
        cfsCloseZkClient(opts);
        return (cb(err));
    });

}

/**
 * @param {Object} opts The options object.
 * @param {String} opts.zk The zookeeper URL.
 * @param {String} opts.path The manatee lock path.
 *
 * Check a zk lock path
 */
function checkLock(opts, cb) {
    var cfs = new ClusterFetchState({
        'zk': opts.zk
    });
    vasync.pipeline({ funcs: [
        cfsCreateZkClient,
        function _checkNode(_, subcb) {
            cfs.zkClient.exists(opts.path, function (err, stat) {
                opts.result = stat;
                return (subcb(err));
            });
        }
    ], arg: cfs}, function (err, results) {
        cfsCloseZkClient(cfs);
        cb(err, opts.result);
    });
}

/**
 * Fetches the history of state changes for this shard from the ZooKeeper
 * cluster, sorts them, and returns them asynchronously via the usual
 * "callback(err, results)" pattern.  If there's no error, then "results" is an
 * array of JavaScript objects denoting a state of the system, including:
 *
 *     time             JavaScript Date object denoting the time this state was
 *                      recorded
 *
 *     state            The system's state (documented with Manatee)
 *
 *     zkSeq            ZooKeeper sequence number for this state.  This is a
 *                      string, possibly with leading zeros.
 *
 * By default, these records are ordered by "zkSeq", which is the order they
 * were recorded in ZooKeeper.  In rare cases when it's useful to order them
 * chronologically instead, use the sortByTime parameter.
 *
 * @param {Object}  args            required arguments
 * @param {Object}  args.zk         ZooKeeper connection string for a cluster
 *                                  storing the history of this shard
 * @param {String}  args.shard      the manatee shard name
 * @param {Boolean} args.sortByTime optional: sort by timestamp rather than ZK
 *                                  sequence number
 */
function history(args, cb) {
    var hist;

    assert.object(args, 'args');
    assert.string(args.zk, 'args.zk');
    assert.string(args.shard, 'args.shard');
    assert.optionalBool(args.sortByTime, 'args.sortByTime');

    hist = {
        'zk': args.zk,
        'shardPath': '/manatee/' + args.shard + '/history',
        'sortByTime': args.sortByTime,
        'zkClient': null,
        'nodes': null,
        'history': null
    };

    vasync.pipeline({ arg: hist, funcs: [
        cfsCreateZkClient,

        function fetchHistory(_, subcb) {
            hist.zkClient.getChildren(hist.shardPath, function (err, c) {
                if (!err)
                    hist.nodes = c;
                return subcb(err);
            });
        },

        function formatNodes(_, subcb) {
            vasync.forEachParallel({
                'func': translateHistoryNode,
                'inputs': hist.nodes.map(function (c) {
                    return ({
                        'zkClient': hist.zkClient,
                        'zkPath': hist.shardPath,
                        'zkNode': c
                    });
                })
            }, function (err, res) {
                if (err) {
                    return (subcb(err));
                }

                hist.history = res.operations.map(function (op) {
                    return (op.result);
                });

                hist.history.sort(function (a, b) {
                    var sa, sb;

                    if (hist.sortByTime) {
                        sa = a.time.getTime();
                        sb = b.time.getTime();
                    } else {
                        sa = parseInt(a.zkSeq, 10);
                        sb = parseInt(b.zkSeq, 10);
                    }

                    return (sa - sb);
                });

                var prev = null;
                hist.history.forEach(function (entry) {
                    entry.comment = annotateHistoryNode(entry, prev);
                    prev = entry;
                });

                return (subcb());
            });
        }
    ]}, function (err, results) {
        cfsCloseZkClient(hist);
        return cb(err, hist.history);
    });
}

// private functions

function createZkClient(connStr, cb) {
    cb = once(cb);
    var zkClient = zk.createClient(connStr);
    zkClient.once('connected', function () {
        LOG.info('zk connected');
        return cb(null, zkClient);
    });

    zkClient.once('disconnected', function () {
        throw new VError('zk client disconnected!');
    });

    zkClient.on('error', function (err) {
        throw new VError(err, 'got zk client error!');
    });

    LOG.info('connecting to zk');
    zkClient.connect();
    setTimeout(function () {
        return cb(new VError('unable to connect to zk'));
    }, 10000).unref();
}

function queryPg(url, query, callback) {
    callback = once(callback);
    LOG.debug({
        url: url,
        query: query
    }, 'query: entering.');

    setTimeout(function () {
        return callback(new VError('postgres request timed out'));
    }, 1000);
    var client = new pg.Client(url);
    client.connect(function (err) {
        if (err) {
            return callback(err);
        }
        LOG.debug({
            sql: query,
            url: url
        }, 'query: connected to pg, executing sql');
        client.query(query, function (err2, result) {
            LOG.debug({err: err2, url: url}, 'returned from query');
            client.end();
            return callback(err2, result);
        });
    });
}

function oldHistoryToObj(fNode) {
    var node = {};
    for (var j = 0; j < fNode.length; j++) {
        var entry = (fNode[j] === null ^ fNode[j] === 'undefined' ^
                     fNode[j] === 'null') ?  '' : fNode[j];
        switch (j) {
        case 0:
            node.time = entry;
            node.date = new Date(parseInt(entry, 10));
            break;
        case 1:
            node.ip = entry;
            break;
        case 2:
            node.action = entry;
            break;
        case 3:
            node.role = entry;
            break;
        case 4:
            node.master = entry;
            break;
        case 5:
            node.slave = entry;
            break;
        case 6:
            node.zkSeq = entry;
            break;
        default:
            break;
        }
    }

    return (node);
}

function translateHistoryNode(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.zkClient, 'opts.zkClient');
    assert.string(opts.zkPath, 'opts.zkPath');
    assert.string(opts.zkNode, 'opts.zkNode');

    // Old entries look like timestamp-ip-role-master-slave-zkseq from zk.
    // New Entries look like generation-zkseq
    var fNode = opts.zkNode.split('-');
    if (fNode.length > 2) {
        return (cb(null, oldHistoryToObj(fNode)));
    }

    var p = opts.zkPath + '/' + opts.zkNode;
    opts.zkClient.getData(p, function (err, data, stat) {
        if (err) {
            return (cb(err));
        }
        var time = bignum.fromBuffer(stat.ctime).toNumber();
        var ret = {
            'time': new Date(time),
            'state': JSON.parse(data.toString('utf8')),
            'zkSeq': fNode[1]
        };
        return (cb(null, ret));
    });
}

/*
 * Given a manatee history node "evt" and previous node "last", return a
 * human-readable string describing what happened between these states.
 */
function annotateHistoryNode(evt, last)
{
    var nst, lst, changes;
    var oldpeers, newpeers, h;

    if (evt.action) {
        return ('v1.0 event');
    }

    if (last === null) {
        if (evt.state.oneNodeWriteMode)
            return ('cluster setup for singleton ' +
                '(one-node-write) mode');
        return ('cluster setup for normal (multi-peer) mode');
    }

    nst = evt.state;
    lst = last.state;

    if (nst.generation < lst.generation) {
        return ('error: gen number went backwards');
    }

    if (!lst.oneNodeWriteMode && nst.oneNodeWriteMode) {
        return ('error: unsupported transition from multi-peer ' +
            'mode to singleton (one-node-write) mode');
    }

    if (lst.oneNodeWriteMode && !nst.oneNodeWriteMode) {
        return ('cluster transitioned from singleton ' +
            '(one-node-write) mode to multi-peer mode');
    }

    if (nst.primary.zoneId != lst.primary.zoneId) {
        if (nst.generation == lst.generation) {
            return ('error: new primary, but same gen number');
        }

        if (lst.sync === null ||
            nst.primary.zoneId != lst.sync.zoneId) {
            return ('error: new primary was not previous sync');
        }

        return (sprintf('sync (%s) took over as primary (from %s)',
            nst.primary.zoneId.substr(0, 8),
            lst.primary.zoneId.substr(0, 8)));
    }

    if (nst.generation > lst.generation) {
        if (nst.sync.zoneId == lst.sync.zoneId) {
            return ('error: gen number changed, but ' +
                'primary and sync did not');
        }

        return (sprintf('primary (%s) selected new sync ' +
            '(was %s, now %s)', nst.primary.zoneId.substr(0, 8),
            lst.sync.zoneId.substr(0, 8),
            nst.sync.zoneId.substr(0, 8)));
    }

    if (nst.sync === null && lst.sync !== null ||
        nst.sync !== null && lst.sync === null ||
        nst.sync.zoneId != lst.sync.zoneId) {
        return ('error: sync changed, but gen number did not');
    }

    changes = [];
    if (nst.freeze && !lst.freeze) {
        changes.push(sprintf('cluster frozen: %s', nst.freeze.reason));
    } else if (!nst.freeze && lst.freeze) {
        changes.push('cluster unfrozen');
    }

    newpeers = {};
    oldpeers = {};
    nst.async.forEach(function (a) { newpeers[a.zoneId] = true; });
    lst.async.forEach(function (a) { oldpeers[a.zoneId] = true; });
    for (h in newpeers) {
        if (!oldpeers.hasOwnProperty(h))
            changes.push(sprintf('async "%s" added',
                h.substr(0, 8)));
    }
    for (h in oldpeers) {
        if (!newpeers.hasOwnProperty(h))
            changes.push(sprintf('async "%s" removed',
                h.substr(0, 8)));
    }

    newpeers = {};
    oldpeers = {};
    nst.deposed.forEach(function (a) { newpeers[a.zoneId] = true; });
    lst.deposed.forEach(function (a) { oldpeers[a.zoneId] = true; });
    for (h in newpeers) {
        if (!oldpeers.hasOwnProperty(h))
            changes.push(sprintf('"%s" deposed',
                h.substr(0, 8)));
    }
    for (h in oldpeers) {
        if (!newpeers.hasOwnProperty(h))
            changes.push(sprintf('"%s" no longer deposed',
                h.substr(0, 8)));
    }

    return (changes.join(', '));
}

/**
 * transform an zk election node name into a backup server url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort-0000000057
 *
 * @return {string} The transformed backup server url, e.g.
 * http://10.0.0.0:5432
 */
function transformBackupUrl(zkNode) {
    var data = zkNode.split('-')[0].split(':');
    return 'http://' + data[0] + ':' + data[2];
}

/**
 * transform an zk election node name into a postgres url.
 * @param {string} zkNode The zknode, e.g.
 * 10.77.77.9:pgPort:backupPort-0000000057
 *
 * @return {string} The transformed pg url, e.g.
 * tcp://postgres@10.0.0.0:5432/postgres
 */
function transformPgUrl(zkNode) {
    var data = zkNode.split('-')[0].split(':');
    return 'tcp://postgres@' + data[0] + ':' + data[1] + '/postgres';
}
