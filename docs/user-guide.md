<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

# Overview

Welcome to the Manatee guide. This guide provides a quick introduction to
Manatee and helps you set up your own Manatee cluster.  We assume that you're
familiar with general Unix concepts as well as administering a replicated
PostgreSQL(PG) cluster.  For background information on HA PostgreSQL, look
[here](http://www.postgresql.org/docs/9.2/static/high-availability.html).


## What is Manatee?

Manatee is a system for automating failure detection and recovery in a cluster
of Postgres intances.  A traditional replicated PG cluster is not resilient to
the death of a node or the partitioning of any of the nodes from the rest of the
cluster.  Instead, when things go wrong, an operator is paged to fix fixes the
cluster, usually by redirecting clients to a secondary peer.  Manatee automates
failover by using a consensus layer (Zookeeper) to identify working nodes and
automatically reconfiguring the cluster in response to node failures.

Manatee also simplifies backups, restores, and rebuilds. New nodes in the
cluster are automatically bootstrapped from existing nodes. If a node becomes
unrecoverrable, it can be rebuilt by restoring from an existing node in the
cluster.

## Architectural Components

### Cluster Overview

This is the component diagram of a fully setup Manatee cluster. Much of the
details of each Manatee node itself has been simplified, with detailed
descriptions in later sections.

![Manatee Architecture](http://us-east.manta.joyent.com/poseidon/public/manatee/docs/Manatee-Shard.jpg "Manatee Cluster")

Clusters typically consist of three Manatee nodes (also called peers): a primary
(A), synchronous standby (B) and asynchronous standby (C) set up in a daisy
chain.  Additional asynchronous nodes can be used as well.

[PostgreSQL Cascading
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#CASCADING-REPLICATION)
is used in the cluster at (3). There is only one direct replication connection to
each node from the node immediately behind it. That is, only B replicates from
A, and only C replicates from B. C will never replicate from A as long as B is
alive.

The primary uses [PostgreSQL synchronous
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#SYNCHRONOUS-REPLICATION)
to replicate to the synchronous standby. This ensures data written to Manatee
will be persisted to at least the primary and sync standby. Without this,
failovers of a node in the cluster can cause data inconsistency between nodes.
The synchronous standby uses asynchronous replication to the async standby.

The topology of the cluster is maintained in Zookeeper.  The primary (A) is
responsible for monitoring the manatee peers that join the cluster and adjusting
the topology to add them as sync or async peers.  The sync (B) is responsible
for adjusting the topology only if it detects via (2) that the primary has
failed.

### Manatee Node Detail

Here are the components encapsulated within a single Manatee node.

![Manatee Node](http://us-east.manta.joyent.com/poseidon/public/manatee/docs/Manatee-Node.jpg "Manatee Node")

#### Manatee Sitter

The sitter is the main process within Manatee. The PG process runs as the
child of the sitter. PG never runs unless the sitter is running. The sitter is
responsible for:

* Managing the underlying PG instance. The PG process runs as a child process
  of the sitter. This means that PG doesn't run unless the sitter is running.
  Additionally, the sitter initializes, restores, and configures the PG instance
  depending on its current role in the cluster. (1)
* Managing cluster topology in ZK if it is the primary or the sync (5).
* ZFS is used by PG to persist data (4). The use of ZFS will be elaborated on
  in a later section.

### Manatee Snapshotter

The snapshotter takes periodic ZFS snapshots of the PG instance. (2) These
snapshots are used to backup and restore the database to other nodes in the
cluster.

### Manatee Backup Server

The snapshots taken by the snapshotter are made available to other nodes by the
REST backupserver. The backupserver, upon receiving a backup request, will send
the snapshots to another node. (3) (6)

### ZFS

Manatee relies on the [ZFS](http://en.wikipedia.org/wiki/ZFS) file system.
Specifically, ZFS [snapshots](http://illumos.org/man/1m/zfs) are used to create
point-in-time snapshots of the PG database. The `zfs send` and `zfs recv`
utilities are used to restore or bootstrap other nodes in the cluster.

# Supported Platforms

Manatee has been tested and runs in production on Joyent's
[SmartOS](http://www.joyent.com/technology/smartos), and should work on most
[Illumos](http://illumos.org) distributions such as OmniOS. Unix-like operating
systems that have ZFS support should work.  BSD has been reported to work, but
others have not been tested.


# Dependencies

You must install the following dependencies before installing Manatee.

## Node.js
Manatee requires [Node.js 0.10.26](http://nodejs.org/download/) or later.

## Zookeeper
Manatee requires [Apache Zookeeper 3.4.x](http://zookeeper.apache.org/releases.html).

## PostgreSQL
Manatee requires [PostgreSQL 9.2.x](http://www.postgresql.org/download/) or later.

## ZFS
Manatee requires the [ZFS](http://en.wikipedia.org/wiki/ZFS) file system.


# Installation

Get the latest Manatee [source](https://github.com/joyent/manatee).
Alternatively, you can grab the release
[tarball](https://github.com/joyent/manatee/releases).

Install the package dependencies via npm install.
```bash
[root@host ~/manatee]# npm install
```

# Configuration

## Set up Zookeeper

Setup a Zookeeper instance by following
[this guide](http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html).

## Manatee Configuration

Manatee comes with sample configs under
[the etc directory](https://github.com/joyent/manatee/tree/master/etc). Manatee
configuration files are JSON-formatted.

The default configuration files have been well tested and deployed in
production. Refrain from tuning them, especially the timeouts, unless you are
sure of the consequences.

For development, there are tools to generate config files to run multiple
sitters on the same host.  See the user guide for details.

## Configuring PostgreSQL

Manatee comes with a default set of PG
[configs](https://github.com/joyent/manatee/tree/master/etc). You'll want to
tune your `postgresql.conf` with parameters that suit your workload. Refer to
the [PostgreSQL documentation](www.postgres.com).

## SMF

The illumos [Service Management Facility](http://www.illumos.org/man/5/smf) can
be used as a process manager for the Manatee processes. SMF provides service
supervision and restarter functionality for Manatee, among other things. There
is a set of sample SMF manifests under
[the smf directory](https://github.com/joyent/manatee/tree/master/smf).

# Administration

This section assumes you are using SMF to manage the Manatee instances and are
running on SmartOS.

## Creating a new Manatee Cluster

The physical location of each Manatee node in the cluster is important. Each
Manatee node should be located on a different _physical_ host than the others
in the cluster, and if possible in different data centers. In this way, failures
of a single physical host or DC will not impact the availability of your
Manatee cluster.

### Setting up SMF

It's recommended that you use a service restarter, such as SMF, with Manatee.
Before running Manatee as an SMF service, you must first import the service
manifest.
``` bash
[root@host ~/manatee]# svccfg import ./smf/sitter.xml
[root@host ~/manatee]# svccfg import ./smf/backupserver.xml
[root@host ~/manatee]# svccfg import ./smf/snapshotter.xml
```

Check the svcs have been imported:
``` bash
[root@host ~]# svcs -a | grep manatee
disabled       17:33:13 svc:/manatee-sitter:default
disabled       17:33:13 svc:/manatee-backupserver:default
disabled       17:33:13 svc:/manatee-snapshotter:default
```

Once the manifests have been imported, you can start the services.
``` bash
[root@host ~/manatee]# svcadm enable manatee-sitter
[root@host ~/manatee]# svcadm enable manatee-backupserver
[root@host ~/manatee]# svcadm enable manatee-snapshotter
```

Repeat these steps on all Manatee nodes. You now have a highly available
automated failover PG cluster.

## Adding a new Node to the Manatee Cluster

Adding a new node is easy.
* Create a new node.
* Install and configure Manatee from the steps above.
* Setup and start the Manatee SMF services.

This new node will automatically determine its position in the cluster and clone
its PG data from its leader. You do not need to manually build the new node.

## Moving Manatee Nodes

In the normal course of maintenance, you may need to move or upgrade the
Manatee nodes in the cluster. You can do this by adding a new node to the cluster,
waiting for it to catch up (using tools described in the following sectons),
and then removing the node you wanted to move/upgrade.

Additional nodes past the initial 3 will just be asynchronous standbys and will
not impact the cluster's performance or availability.

## Cluster Administration

Manatee provides the `manatee-adm` utility which gives visibility into the
status of the Manatee cluster. One of the key subcommands is the `status` command.
``` bash
[root@host ~/manatee]# ./node_modules/manatee/bin/manatee-adm status -z <zk_ip>
{
"1": {
    "primary": {
        "id": "172.27.4.12:5432:12345",
        "ip": "172.27.4.12",
        "pgUrl": "tcp://postgres@172.27.4.12:5432/postgres",
        "zoneId": "b515b611-3b6e-4cc9-8d1f-3cf78e27bf24",
        "backupUrl": "http://172.27.4.12:12345",
        "online": true,
        "repl": {
            "pid": 23047,
            "usesysid": 10,
            "usename": "postgres",
            "application_name": "tcp://postgres@172.27.5.8:5432/postgres",
            "client_addr": "172.27.5.8",
            "client_hostname": "",
            "client_port": 42718,
            "backend_start": "2014-04-28T17:07:55.548Z",
            "state": "streaming",
            "sent_location": "E/D9D14780",
            "write_location": "E/D9D14780",
            "flush_location": "E/D9D14780",
            "replay_location": "E/D9D14378",
            "sync_priority": 1,
            "sync_state": "sync"
        }
    },
    "sync": {
        "id": "172.27.5.8:5432:12345",
        "ip": "172.27.5.8",
        "pgUrl": "tcp://postgres@172.27.5.8:5432/postgres",
        "zoneId": "d2de0030-986e-4dae-991d-7d85f2e333d9",
        "backupUrl": "http://172.27.5.8:12345",
        "online": true,
        "repl": {
            "pid": 95136,
            "usesysid": 10,
            "usename": "postgres",
            "application_name": "tcp://postgres@172.27.3.7:5432/postgres",
            "client_addr": "172.27.3.7",
            "client_hostname": "",
            "client_port": 53461,
            "backend_start": "2014-04-28T21:28:07.905Z",
            "state": "streaming",
            "sent_location": "E/D9D14780",
            "write_location": "E/D9D14780",
            "flush_location": "E/D9D14780",
            "replay_location": "E/D9D14378",
            "sync_priority": 0,
            "sync_state": "async"
        }
    },
    "async": {
        "id": "172.27.3.7:5432:12345",
        "ip": "172.27.3.7",
        "pgUrl": "tcp://postgres@172.27.3.7:5432/postgres",
        "zoneId": "70d44638-f4fb-4cbd-8611-0f7c83d8502f",
        "backupUrl": "http://172.27.3.7:12345",
        "online": true,
        "repl": {},
        "lag": {
            "time_lag": {}
        }
    }
}
}
```
This prints out the current topology of the cluster.  The "online" filed indicates
that postgres is online.

The "repl" field shows the standby that's currently connected to the node. It's
the same set of fields that is returned by the PG query ```select * from
pg_stat_replication```. The repl field is helpful in verifying the status of the
PostgreSQL instances. If the repl field exists, it means that the node's standby
is caught up and streaming. If there is no repl field, it means that there is no
PG instance replication from this node. This is expected with the last node in
the cluster.

However, if there is another node after the current one in the cluster, and there
is no repl field, it indicatees there is a problem with replication between the
two nodes.

## Cluster History
You can query any past topology changes by using the `history` subcommand.
```bash
[root@host ~/manatee]# ./node_modules/manatee/bin/manatee-adm history -s '1' -z <zk_ips>
```

This will return a list of every single cluster state change sorted by time.

## Getting the Mantee version

```
[root@b35e12da (postgres) ~]$ manatee-adm version
2.0.0
```

## Freezing and Unfreezing Clusters

As an operator, there may be times when you want to "freeze" the topoplogy to
keep Manatee from failing over the Postgres sync to Primary.  For example, this
could be due to expected operational maintenance or because the cluster is in
the middle of a migration.  To freeze the cluster state so that it will perform
no transitions, use the `manatee-adm freeze -r [reason]` command.  The reason is
free-form and required.  The reason is meant for operators to consult before
unfreezing the cluster.  When the cluster is frozen, it is prominently displayed
in the output for `manatee-adm status`:

```
[root@b35e12da (postgres) ~]$ manatee-adm freeze -r 'By nate for CM-129'
Frozen.
[root@b35e12da (postgres) ~]$ manatee-adm status | json | head -5
{
  "1.moray.coal.joyent.us": {
    "__FROZEN__": "2014-12-10T18:20:35.758Z: By nate for CM-129",
    "primary": {
      "id": "10.77.77.8:5432:12345",
[root@b35e12da (postgres) ~]$
```

To unfreeze the cluster, use the `manatee-adm unfreeze` command.

## Deposed manatees

When a sync takes over becoming the primary, there is a chance that the previous
primary's Postgres transaction logs have diverged.  There are many reasons this
can happen, and the reasons we know about are documented in the [transaction log
divergence](xlog-diverge.md) doc.  Deposed manatees have the "deposed" tag when
viewing `manatee-adm status`:

***TODO***

To bring a deposed node back into service requires rebuilding it from one of the
other peers.  To rebuild a deposed node, log onto the host, run `manatee-adm
rebuild`, and follow the prompts.  This process can take anywhere from seconds
to hours depending on the size of the dataset and the available network
bandwidth between nodes.  Consider running the rebuild in a `screen` session.

To be on the safe side, any deposed primary should be rebuilt in order to rejoin
the cluster.  This may not be necessary in some cases, but is suggested unless
you can determine manually that the Postgres transaction logs (xlogs) haven't
diverged.

The `manatee-adm remove_deposed` command is provided to explicitly unmark a node
as deposed.  This is primarily intended for cases where the node is being fully
removed from the cluster.  ***WARNING:*** If you do unmark a node as deposed,
but do not rebuild it, and then add the node back into the cluster, the node
will get picked up as an async and your cluster is at risk of becoming wedged!
This is because this node's xlogs may have diverged, so it cannot receive
replication updates from other peers, but you've overridden the mechanism that
prevents the system from doing just that.  If that happens, your cluster will
not be able to accept writes.  To avoid this, do not use the `remove_deposed`
subcommand unless you're actually removing a deposed peer from the cluster.

## One Node Write Mode

One node write mode (ONWM) is a special mode for manatee clusters that have no
important data.  It allows a primary to accept writes without synchronously
replicating those trasactions to a sync.  In fact, with ONWM, any peers that
attempt to join the cluster will shut down if they detect another peer is
already the primary by looking at the cluster state in zookeeper.

ONWM is usually enabled when setting up larger systems that will me moved to
"High Availability" (HA) configurations later.  To move from ONWM to HA requires
downtime.  An operator would:

1. Provision a new manatee peer and configure it as part of the primary's
   cluster.
2. Stop the primary manatee sitter process.
3. Edit configuration for the primary to remove one node write mode.
4. Run `# manatee-adm onwm -m off`
5. Start the primary manatee sitter process.

It is generally not supported or advised to go from an HA manatee cluster to
ONWM.
