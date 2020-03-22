Ghostferry
==========

Ghostferry is a library that enables you to selectively copy data from one mysql instance to another with minimal amount of downtime.

It is inspired by Github's [gh-ost](https://github.com/github/gh-ost),
although instead of copying data from and to the same database, Ghostferry
copies data from one database to another and have the ability to only
partially copy data.

There is an example application called ghostferry-copydb included (under the
`copydb` directory) that demonstrates this library by copying an entire
database from one machine to another.

Talk to us on IRC at [irc.freenode.net #ghostferry](https://webchat.freenode.net/?channels=#ghostferry).

- **Tutorial and General Documentations**: https://shopify.github.io/ghostferry
- Code documentations: https://godoc.org/github.com/Shopify/ghostferry

Stable Versus Experimental
--------------------------

This project is an *experimental *fork of the official
[ghostferry project](https://github.com/Shopify/ghostferry). We add various
features and fixes that have not (yet) made it into the upstream version.

While the changes added in this fork **are considered experimental** and must
only be used at your own risk, we use them on a daily basis and we consider
them stable for use in practice. Still, use at your own risk and opt for the original version if their features satisfy your needs.

If you discover bugs in any feature added in this fork, please open an issue
ticket with the details. If the issue is within the base system, open an issue
on the original project - we constantly monitor stable changes there and
incorporate them into this fork.

Features/fixes added in this fork include

- fix [replication instability](https://github.com/Shopify/ghostferry/issues/165)
  for unsigned bigint primary keys: this fix has not made it into upstream
  master yet.
- fix [data corruption for binary columns](https://github.com/Shopify/ghostferry/issues/157):
  this fix has not made it into upstream master yet.
- fix [failure to resume copy](https://github.com/Shopify/ghostferry/issues/156):
  this fix has not made it into upstream master yet.
- support [schema modifications during the cutover phase](https://fillme):
  unlike the original version of `Ghostferry`, which ignores any DDL events (such
  as `CREATE`/`ALTER`/`DELETE TABLE` or `TRUNCATE TABLE` statements), this fork
  propagates such events from the source to the target database.
  Note that there are a few restrictions with this: schema changes occurring during
  the *copy phase* trigger the delay of binlog replication until the copy has
  completed. Further, data integrity verification is not supported once a schema
  is changed.
- support [writing resume/state data to file](https://github.com/Shopify/ghostferry/issues/163)
  instead of using *stdout*.
- support [copying tables without "paging" primary keys](https://github.com/Shopify/ghostferry/issues/162):
  `Ghostferry` requires integer auto-increment primary keys for copying data.
  An optional feature in this fork allows marking tables for "full copy",
  allowing to copy tables that do not meet this primary key requirement. It is
  **strongly recommended** to use this feature with care and only on tables with
  few rows, as the copy process requires locking the entire table on the source
  database.
- improved handling of [foreign key constraints](https://github.com/Shopify/ghostferry/issues/161):
  `Ghostferry` does not allow copying tables using *foreign key constraints*,
  because it copies data in batches, which is likely to violate constraints,
  leading to failures during the copy phase.  
  The forked version supports inferring the order in which tables need to be
  created in the target database. This allows working around constraints in the
  setup phase. Additionally disabling foreign key constraint enforcement on the
  target database session/connection allows working around constraints during
  the copy phase.  
  Note that an *incomplete* execution of `Ghostferry`  will leave the database in
  an inconsistent state until the copy is resumed and completed.  
  Part of this feature is in review into the upstream project already, but once
  the review is merged, it will only allow to **manually** specify the order of
  tables to be created. The fork **automatically** infers this order for the user.

Overview of How it Works
------------------------

An overview of Ghostferry's high-level design is expressed in the TLA+
specification, under the `tlaplus` directory. It maybe good to consult with
that as it has a concise definition. However, the specification might not be
entirely correct as proofs remain elusive.

On a high-level, Ghostferry is broken into several components, enabling it to
copy data. This is documented at
https://shopify.github.io/ghostferry/master/technicaloverview.html

Development Setup
-----------------

Install:

- Have Docker installed
- Clone the repo
- `docker-compose up -d mysql-1 mysql-2`

Run tests:

- `make test`

Test copydb:

- `make copydb && ghostferry-copydb -verbose examples/copydb/conf.json`
- For a more detailed tutorial, see the
  [documentation](https://shopify.github.io/ghostferry).

Ruby Integration Tests
----

Kindly take note of following options:
*   `DEBUG=1`: To see more detailed debug output by `Ghostferry`
*   `TESTOPTS=...`: As detailed under https://docs.ruby-lang.org/en/2.1.0/Rake/TestTask.html

Example:
`bundle exec rake test DEBUG=1 TESTOPTS="-v --name=TrivialIntegrationTests#test_logged_query_omits_columns"`
