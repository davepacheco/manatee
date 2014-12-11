<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Manatee

                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

This repository is part of the Joyent SmartDataCenter project (SDC).  For
contribution guidelines, issues, and general documentation, visit the main
[SDC](http://github.com/joyent/sdc) project page.

## Overview

Manatee is a system for clustering PostgreSQL instances for better durability
and availability than you'd get from a single instance.  In terms of CAP,
Manatee is built to support a strongly consistent (CP) system: at any given
time, only one PostgreSQL instance (the primary peer) serves client requests.
If the primary fails or becomes partitioned, another peer will automatically
take over (i.e., without operator intervention).  Synchronous replication is
used to avoid any data loss in the event of a takeover.

Postgres clients are responsible for using the [manatee client
library](https://github.com/joyent/node-manatee) to locate the current primary
peer.  Manatee is typically deployed with
[Moray](https://github.com/joyent/moray), a key-value store that uses the
Manatee client to keep up with Manatee cluster changes and hides these details
from its clients.  Manatee does not manage IP-level failover or anything like
that, though PostgreSQL is configured so that accidental writes to a non-primary
will never complete successfully.

To ensure data integrity, Manatee is built atop
[ZFS](http://en.wikipedia.org/wiki/ZFS) and [PostgreSQL synchronous
replication](http://www.postgresql.org/docs/9.2/static/warm-standby.html#SYNCHRONOUS-REPLICATION).


## Documentation

Check out the [user-guide](docs/user-guide.md) for details on server internals
and setup.

Problems? Check out the [Troubleshooting guide](docs/trouble-shooting.md).

Migrating from Manatee 1.0 to 2.0?  Check out the [migration
guide](docs/migrate-1-to-2.md).

If you want to build and test Manatee yourself, check out the [developer's
guide](docs/dev-guide.md).


## Client quick start

The client library is used to locate the current primary peer in the cluster.
Detailed client docs are [here](https://github.com/joyent/node-manatee).

```javascript
var manatee = require('node-manatee');

var client = manatee.createClient({
   "path": "/manatee/1",
   "zk": {
       "connStr": "172.27.10.97:2181,172.27.10.90:2181,172.27.10.101:2181",
       "opts": {
           "sessionTimeout": 60000,
           "spinDelay": 1000,
           "retries": 60
       }
   }
});

client.once('ready', function () {
    console.log('manatee client ready');
});

client.on('topology', function (urls) {
    console.log({urls: urls}, 'topology changed');
    /* start using the new primary */
});

client.on('error', function (err) {
    console.error({err: err}, 'got client error');
});
```


## Features and design notes

These notes are for helping understand Manatee's design.  For details on
actually operating Manatee, see the user guide.

#### Automated cluster setup

A cluster is automatically set up when two peers connect to the ZooKeeper
cluster.  As new peers are added, they're automatically added to the cluster.


#### Durability

Manatee clusters typically contain three nodes: a primary, a synchronous peer
("sync"), and an asynchronous peer ("async").  The primary replicates
synchronously to the sync, which means that transactions cannot commit on the
primary unless they're also recorded on the sync.  This ensures that loss of the
primary never loses data, and the sync can take over as primary at any time
without losing data.


#### Automated failure detection and recovery

Failure is determined by loss of connectivity to the ZooKeeper cluster.  If the
primary fails, the sync takes over as primary, and the async takes over as sync.
The cluster can serve reads nearly the entire time, though Postgres clients need
to redirect reads to the new primary.  The cluster will be able to serve writes
as soon as the async catches up, which is typically in a few seconds, even in
busy systems.

Due to limitations of Postgres, when a primary is removed from the cluster, it's
usually not possible to re-add it to the cluster without losing data.  Manatee
never does this automatically.  As a result, operator intervention is required
to bring the cluster back to the same number of peers.  See the user guide for
details.

Because of this, each asynchronous peer that you add to the cluster increases
the number of peers you can lose without requiring operator intervention to
recover the cluster.  No matter how many peers are part of the cluster, the
cluster can continue operating as long as only two peers are operating (a
primary and sync) and as long as you've never lost a primary *and* sync before a
new sync was able to catch up.


#### Avoiding split-brain

The primary is the only database instance that's writable, and it's always
configured for synchronous replication.  This prevents a split-brain situation.


#### Surviving availability zone failure

In some production deployments, multiple peers for the same Manatee cluster are
deployed in separate availability zones within the same region.  In practice,
we mean that the availability zones share no common physical components, but
they do share low-latency, high-bandwidth network connections between them.  In
this way, a Manatee configuration can survive failures or partition events that
affect entire availability zones.


## Production deployments

Manatee is a core component of Joyent's
[SmartDataCenter](http://github.com/joyent/sdc) as well as the
[Manta](http://github.com/joyent/manta) storage service.  It's running in
production in all of Joyent's datacenters, as well as SDC and Manta customers.
