# pg_logical_cdc

pg_logical_cdc captures change data of a PostgreSQL in a programmable manner. It dumps
[logical replication log stream](https://www.postgresql.org/docs/11/logical-replication.html) to
STDOUT so that your application can capture changes applied to PostgreSQL.

pg_logical_cdc dumps a change record with a offset of the record in WAL (called `LSN`). Your application
can use the offset as the unique identifier of the record. Also, once your application certainly consumes
a record and doesn't need the same record again, put the offset to STDIN of pg_logical_cdc (See Protocol
section for details). Then, pg_logical_cdc feedback the offset to PostgreSQL server so that replication
resumes from the offset when your application crashes & restarts. This ensures that your application
receives all change data at least once.

PostgreSQL's replication protocol allows to receive change data only from one client node at a time.
pg_logical_cdc supports poll-mode so that one of stand-by nodes can take over change data capturing
immediately when the active node crashes.

pg_logical_cdc is similar to [pg_recvlogical](https://www.postgresql.org/docs/11/app-pgrecvlogical.html).
pg_recvlogical writes captured change data to a file as a stand-alone tool. pg_logical_cdc is
designed to run as a subprocess of an application.

## Usage

```
Usage: --slot=NAME [OPTION]...
Options:
  -?, --help                   show usage
  -v, --verbose                show verbose messages
  -S, --slot NAME              name of the logical replication slot
  -o, --option KEY[=VALUE]     pass option NAME with optional value VALUE to the replication slot
  -c, --create-slot            create a replication slot if not exist using the plugin set to --P option
  -L, --poll-mode              check availability of the replication slot then exit
  -D, --fd INTEGER             use the given file descriptor number instead of 1 (stdout)
  -F, --feedback-interval SECS maximum delay to send feedback to the replication slot (default: 0.000)
  -s, --status-interval SECS   time between status messages sent to the server (default: 1.000)
  -A, --auto-feedback          send feedback automatically
  -H, --write-header           write a header line every before a record
  -N, --write-nl               write a new line character every after a record
  -j, --wal2json1              equivalent to -o format-version=1 -o include-lsn=true -P wal2json
  -J  --wal2json2              equivalent to -o format-version=2 --write-header -P wal2json

Create slot options:
  -P, --plugin NAME            logical decoder plugin for a new replication slot (default: test_decoding)

Poll mode options:
  -u, --poll-duration SECS     maximum amount of time to wait until slot becomes available (default: no limit)
  -i, --poll-interval SECS     interval to check availability of a slot (default: 1.000)

Connection options:
  -d, --dbname DBNAME      database name to connect to
  -h, --host HOSTNAME      database server host or socket directory
  -p, --port PORT          database server port
  -U, --username USERNAME  database user name
  -m, --param KEY=VALUE    database connection parameter (connect_timeout, application_name, etc.)
```

Notice that a logical replication slot must be created in advance. See "Output format" section for details.

### Setting password

Using `PGPASSWORD` environment variable is the recommended way to set password. For example,

```
export PGUSER=foo
export PGPASSWORD=foobar
pg_logical_cdc --slot my_slot -HNJ
```

You can find the list of available environment variables in [libpq document](https://www.postgresql.org/docs/11/libpq-envars.html).

pg_logical_cdc doesn't ask for password because STDIN is usually a program.

Additional connection parameters can be set using `-m KEY=VALUE` option. Available parameters are listed in [libpq document](https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

## Protocol

### Output format

Format varies depending on the [logical decoding plugin](https://www.postgresql.org/docs/11/logicaldecoding-explanation.html).
The most recommended and tested plugin is [wal2json](https://github.com/eulerto/wal2json) which formats change data in JSON.

To use a decoding plugin, you create a replication slot first with the plugin name.
For example, run following statement on PostgreSQL:

```
select * from pg_create_logical_replication_slot('test_slot', 'wal2json');
```

It creates a slot named `test_slot` using `wal2json` plugin.
Then, you can start pg_logical_cdc with the slot name:

```
pg_logical_cdc --slot test_slot
```

You will see JSON outputs when you make changes on tables.

Example output is:

```
$ ./pg_logical_cdc --slot test_slot -N
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[108,5,5,5,5]}]}
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[109,5,5,5,5]}]}
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[110,5,5,5,5]}]}
...
```

### Output header

If you give `--write-header` option, pg_logical_cdc dumps a record with a header.

Format of a header is:

```
w <LSN> <LENGTH>\n
```

* `<LSN>` is the offset of the record. Format of a LSN is "%X/%X" (slash-separated two integers in hex format).

* `<LENGTH>` is the length of a change record followed by the header.

* `\n` is a new-line character.

Example output is:

```
$ ./pg_logical_cdc --slot test_slot -N --write-header
w 0/2B357690 196
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[108,5,5,5,5]}]}
w 0/2B357890 196
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[109,5,5,5,5]}]}
...
```

### Feedback command

Send a feedback command to STDIN for sending a feedback message.

Format of a feedback command is:

```
F <LSN>\n
```

* `<LSN>` is the last LSN received as a part of output header.

* `\n` is a new-line character.

### Quit command

Send quit command to STDIN for shutting down.

Format of a quit command is:

```
q\n
```

### SIGINT signal

Sending SIGINT signal exits pg_logical_cdc. However, quit command is recommended
because SIGNAL may arrive earlier than processing a feedback command buffered in the
pipe. To make sure that feedback is sent to PostgreSQL, use quit command instead.


## Poll mode

If `--poll-mode` is set, pg_logical_cdc runs in poll mode. Poll mode is useful
for HA configuration - a backup node takes over replication immediately when active node
crashes.

pg_logical_cdc running in poll mode doesn't output records. When it exits wit
code 0 (SUCCESS), run pg_logical_cdc again without poll mode.

If maximum amount of time passes (`--poll-duration` option), pg_logical_cdc exits
with exit code 9 (SLOT_IN_USE). It may also exit with 8 (SLOT_NOT_EXIST) if `--create-slot`
is not set. If 0 is set to `--poll-duration`, it exits immediately after the first check.

Example use of poll-mode is as following:

```
#!/bin/sh

while true; do
  # Run in poll mode.
  pg_logical_cdc --slot test_slot -J --poll-mode --poll-duration 60 --create-slot
  ecode=$?

  # If exist code is 0, slot is ready. Run pg_logical_cdc without poll mode.
  if [ $ecode -eq 0 ]; then
    pg_logical_cdc --slot test_slot -J
    ecode=$?
  fi

  # If exist code not is 9 (SLOT_IN_USE), thre was an error.
  if [ $ecode -nq 9 ]; then
    exit $ecode
  fi

  # If exit code is 9 (SLOT_IN_USE), slot is not ready. Retry poll mode again.
done
```

## Exit code

* 0 = SUCCESS. Command exited with no errors.
* 1 = INVALID_ARGS. Command exited before attempting to establish a PostgreSQL connection.
* 2 = INIT_FAILED. An error occurred during establishing or initializing a PostgreSQL connection.
* 3 = PG_CLOSED. PostgreSQL connection is closed.
* 4 = CMD_CLOSED. STDIN is closed.
* 5 = PG_ERROR. An error occurred during dealing with the PostgreSQL connection.
* 6 = CMD_ERROR. An error occurred during dealing with STDIN.
* 7 = SYSTEM_ERROR. Other fatal errors.
* 8 = SLOT_NOT_EXIST. Replication slot does not exist.
* 9 = SLOT_IN_USE. Replication slot is being used by another client.

## Example code

```
#!/usr/bin/env ruby

# Start pg_logical_cdc
pipe = IO.popen("pg_logical_cdc --slot test_slot --wal2json2", "r+")

while true
  # Receive a header line from STDOUT
  header = pipe.gets

  # Parse the header line
  w, lsn, length = header.split(" ")

  # Read a change record from STDOUT
  record = pipe.read(length.to_i)

  # Use the record
  yield({unique_id: lsn, record: record})

  # Feedback LSN to STDIN
  pipe.puts "F #{LSN}"
end
```


## Development

### Build and test using docker

Docker is the recommended way to run build and tests reliably.

```
make docker
```

You will get `pg_logical_cdc` in src/.

### Build without docker

Please make sure that `libpq` is installed with development headers.

* On Ubuntu and Debian, you can run `apt-get install libpq-dev` to install.

* On Mac OS X with Homebrew, you can run `brew install postgresql` to install.

Once libpq is installed, run `make` on ./src directory as following:

```
make -C ./src
```

You will get `pg_logical_cdc` in ./src directory.

### Test without docker

Pre-requirements:

* A running PostgreSQL server, with a database created, and running with wal_level=logical
* libpq installed with development headers
* ruby installed with `bundler` gem

```
# First, set environment variables for the running PostgreSQL.
$ export PGHOST=<PostgreSQL hostname>
$ export PGUSER=<PostgreSQL username>
$ export PGDATABASE=<PostgreSQL database name>

# Then, run "make test"
$ make test

# Running one test only
$ SPEC=spec/run_spec.rb:117 make test
```

## License

Copyright (c) 2020 Sadayuki Furuhashi

Apache License, Version 2.0

