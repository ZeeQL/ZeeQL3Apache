<h2>APR and Apache Database Adaptors for ZeeQL3
  <img src="http://zeezide.com/img/ZeeQLIcon1024-QL.svg"
       align="right" width="128" height="128" />
</h2>

![Apache 2](https://img.shields.io/badge/apache-2-yellow.svg)
![Swift3](https://img.shields.io/badge/swift-3-blue.svg)
![macOS](https://img.shields.io/badge/os-macOS-green.svg?style=flat)
![tuxOS](https://img.shields.io/badge/os-tuxOS-green.svg?style=flat)
![Travis](https://travis-ci.org/ZeeQL/ZeeQL3Apache.svg?branch=develop)

This library contains ZeeQL database adaptors based on the Apache Portable
Runtime DBD module. APR DBD is kinda like a mini-ODBC/JDBC and has drivers
for various databases. It supports prepared statements, transactions and
some other stuff, but is, overall pretty limited.
Note that you can also get the a handle to the native database library (e.g.
libpq).

The primary advantage of APR DBD is that you can easily use it inside Apache
using `mod_swift` and `mod_dbd`. Which gives you two big advantages:

- Apache will manage the connection pool for you, even across language
  environments (e.g. you could write some pages in PHP)
- The web admin can configure the database using regular Apache configuration
  mechanisms.

Note that while DBD abstracts the database client library, we still carry
custom per-database adaptor subclasses. E.g. `APRPostgreSQLAdaptor`. This
is to support database schema reflection, which varies between SQL databases.

## Apache mod_dbd Adaptor

TODO


## Installing APR DBD

### Module Map

To use APR in Swift you need a proper CLang module map for it. 
Sample module maps are included.

### macOS

The system APR DBD included in macOS is statically linked and only carries
the SQLite3 driver. That is, you cannot use it to access a PostgreSQL database.

We suggest using [Homebrew](https://brew.sh) to get access to a full featured 
APR. To install APR with the PG and SQLite3 adaptor via Homebrew:

    brew install apr-util --with-openldap --with-postgresql --with-sqlite

If you already installed APR (or Apache) before, you may need to use
`reinstall` instead of `install`.

Note: you can also just compile APR from [the sources](https://apr.apache.org),
this has the advantage that you can debug the thing from within Xcode (i.e.
step into the APR code).

### Ubuntu / Debian

To install APR on Linux, do something like this:

    sudo apt-get install libaprutil1-dbd-sqlite3 libaprutil1-dbd-pgsql

### Documentation

ZeeQL Documentation can be found at:
[docs.zeeql.io](http://docs.zeeql.io/).

### Who

**ZeeQL** is brought to you by
[ZeeZide](http://zeezide.de).
We like feedback, GitHub stars, cool contract work,
presumably any form of praise you can think of.

There is a `#zeeql` channel on the [Noze.io Slack](http://slack.noze.io).
