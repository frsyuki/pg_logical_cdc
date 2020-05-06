require 'rspec'
require 'pg'

# Set libpq time zone to UTC
ENV['PGTZ'] = 'UTC'

def pg_create_slot(slot_name)
  pg_exec("select pg_create_logical_replication_slot('#{slot_name}', 'wal2json')")
end

def pg_drop_slot(slot_name)
  pg_exec("select pg_drop_replication_slot('#{slot_name}')")
end

def pg_exec(stmt)
  conn = PG.connect
  begin
    r = conn.exec(stmt)
    yield r if block_given?
  ensure
    conn.finish
  end
end

def cmd(slot_name, args="", &block)
  cmd = TestCommand.new(slot_name, args)
  begin
    block.call(cmd)
  ensure
    cmd.finish
  end
end

class TestCommand
  def initialize(slot_name, args="")
    cmd = "#{ENV['EXE']} --slot #{slot_name} -D 3 #{args}"

    stdin_r, @stdin= IO.pipe
    @stdout, stdout_w = IO.pipe
    @stderr, stderr_w = IO.pipe
    @pid = Process.spawn(cmd, 0=>stdin_r, 1=>stdout_w, 2=>stderr_w, 3=>stdout_w)
    stdin_r.close
    stdout_w.close
    stderr_w.close
  end

  attr_reader :stdin, :stdout, :stderr

  def finish
    Process.kill("TERM", @pid)
    Process.waitpid(@pid)
    @stdin.close if @stdin
    @stdout.close if @stdout
    @stderr.close if @stderr
  end
end
