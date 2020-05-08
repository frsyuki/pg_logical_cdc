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
    if block_given?
      yield(r)
    else
      r
    end
  ensure
    conn.finish
  end
end

def cmd(slot_name, args="", &block)
  cmd = TestCommand.new(slot_name, args)
  stat = nil
  begin
    block.call(cmd)
  ensure
    stat = cmd.finish
  end
  stat
end

class TestCommand
  def initialize(slot_name, args="")
    cmd = "#{ENV['EXE']} --slot #{slot_name} -D 3 #{args}"

    stdin_r, @stdin = IO.pipe
    @stdout, stdout_w = IO.pipe
    @stderr_pipe, stderr_w = IO.pipe
    @pid = Process.spawn(cmd, 0=>stdin_r, 1=>stdout_w, 2=>stderr_w, 3=>stdout_w)
    stdin_r.close
    stdout_w.close
    stderr_w.close

    @stderr = ""

    Thread.new do
      begin
        while true
          @stderr << @stderr_pipe.readpartial(2048)
        end
      rescue EOFError => e
      end
    end
  end

  attr_reader :stdin, :stdout, :stderr, :stderr_pipe

  def finish
    Process.kill("TERM", @pid)
    Process.waitpid(@pid)
    stat = $?
    @stdin.close if @stdin
    @stdout.close if @stdout
    @stderr_pipe.close if @stderr_pipe
    return stat
  end
end
