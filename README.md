# pg_logical_stream

pg_logical_stream captures change data of a PostgreSQL in a programmable manner. It dumps
[logical replication log stream](https://www.postgresql.org/docs/11/logical-replication.html) to STDOUT so that your application can capture changes made on PostgreSQL.

pg_logical_stream dumps a change record with a offset of the record (called `LSN`). Your application
can use the offset as the unique identifier of the record. Also, once your application certainly consumes
a record and doesn't need the same record again, put the offset to STDIN of pg_logical_stream (See Protocol
section for details). Then, pg_logical_stream sends the offset to PostgreSQL server so that replication
resumes from the offset when your application crashes & restarts.

pg_logical_stream is similar to [pg_recvlogical](https://www.postgresql.org/docs/11/app-pgrecvlogical.html). pg_recvlogical writes captured
change data to a file as a stand-alone tool, where pg_logical_stream is designed to run as a subprocess of an application.

## Usage

```
Usage: --slot=NAME [OPTION]...
Options:
  -?, --help                   show usage
  -v, --verbose                show verbose messages
  -S, --slot NAME              name of the logical replication slot
  -o, --option KEY[=VALUE]     pass option NAME with optional value VALUE to the replication slot
  -D, --fd INTEGER             use the given file descriptor number instead of 1 (stdout)
  -F, --feedback-interval SEC  maximum delay to send feedback to the replication slot (default: 0.000)
  -s, --status-interval SECS   time between status messages sent to the server (default: 5.000)
  -A, --auto-feedback          send feedback automatically
  -H, --write-header           write a header line every before a record
  -N, --write-nl               write a new line character every after a record
  -j, --wal2json1              equivalent to -o format-version=1 -o include-lsn=true
  -J  --wal2json2              equivalent to -o format-version=2 --write-header

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
pg_logical_stream --slot my_slot -HNJ
```

You can find the list of available environment variables in [libpq document](https://www.postgresql.org/docs/11/libpq-envars.html).

pg_logical_stream doesn't ask for password because STDIN is usually a program.

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
Then, you can start pg_logical_stream with the slot name:

```
pg_logical_stream --slot test_slot
```

You will see JSON outputs when you make changes on tables.

Example output is:

```
$ ./pg_logical_stream --slot test_slot -N
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[108,5,5,5,5]}]}
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[109,5,5,5,5]}]}
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[110,5,5,5,5]}]}
...
```

### Output header

If you give `--write-header` option, pg_logical_stream dumps a record with a header.

Format of a header is:

```
w <LSN> <LENGTH>\n
```

* `<LSN>` is the offset of the record. Format of a LSN is "%X/%X" (slash-separated two integers in hex format).

* `<LENGTH>` is the length of a change record followed by the header.

* `\n` is a new-line character.

Example output is:

```
$ ./pg_logical_stream --slot test_slot -N --write-header
w 0/2B357690 196
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[108,5,5,5,5]}]}
w 0/2B357890 196
{"change":[{"kind":"insert","schema":"public","table":"test","columnnames":["id","n1","n2","n3","n4"],"columntypes":["integer","bigint","bigint","bigint","bigint"],"columnvalues":[109,5,5,5,5]}]}
w 0/2B3586D0 196
...
```

### Input command

Format of a feedback command is:

```
F <LSN>\n
```

* `<LSN>` is the LSN received as a part of output header.

* `\n` is a new-line character.

## Example code

```
#!/usr/bin/env ruby

# Start pg_logical_stream
pipe = IO.popen("pg_logical_stream --slot test_slot --wal2json2", "r+")
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
pipe
```


## Development

### Build and test using docker

```
make clean docker
```

You will get `pg_logical_stream` in src/.

### Build

Please make sure that `libpq` is installed with development headers.

* On Ubuntu and Debian, you can run `apt-get install libpq-dev` to install.

* On Mac OS X with Homebrew, you can run `brew install postgresql` to install.

Once libpq is installed, run `make` on ./src directory as following:

```
make -C ./src
```

You will get `pg_logical_stream` in ./src directory.

### Test

Pre-requirements:

* A running PostgreSQL server with a database created
* libpq installed with development headers
* ruby installed with `bundler` gem

```
# First, set environment variables for the running PostgreSQL.
$ export PGHOST=<PostgreSQL hostname>
$ export PGUSER=<PostgreSQL username>
$ export PGDATABASE=<PostgreSQL database name>

# Then, run "make test"
$ make test
```

