require_relative 'spec_helper'

HEADER_REGEXP = /^w (?<lsn>[0-9A-F]+\/[0-9A-F]+) (?<len>[0-9]+)$/

RSpec.describe "run" do
  let(:suffix) do
    #"%08x" % rand(2**32)
    "z"
  end

  let(:slot_name) do
    "pg_logical_stream_test_slot_#{suffix}"
  end

  let(:table1) do
    "pg_logical_stream_test_t1_#{suffix}"
  end

  let(:table2) do
    "pg_logical_stream_test_t2_#{suffix}"
  end

  before(:each) do
    pg_drop_slot(slot_name) rescue nil
    pg_exec "drop table if exists #{table1}"
    pg_exec "drop table if exists #{table2}"
    pg_exec <<~SQL
      create table if not exists #{table1} (
        id bigserial primary key,
        name text not null,
        extra bytea
      )
    SQL
    pg_exec <<~SQL
      create table if not exists #{table2} (
        id serial primary key,
        c_date date,
        c_timestamp timestamp,
        c_timestamptz timestamptz
      )
    SQL
    pg_create_slot(slot_name)
  end

  after(:each) do
    pg_drop_slot(slot_name)
    pg_exec "drop table if exists #{table1}"
    pg_exec "drop table if exists #{table2}"
  end

  it "capture inserts" do
    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table1} (name) values ('n1'), ('n2')"

      # Begin ("B")
      h1 = c.stdout.gets
      r1 = c.stdout.gets

      expect(h1).to match(HEADER_REGEXP)
      h1_len = HEADER_REGEXP.match(h1)[:len].to_i
      expect(h1_len).to eq(r1.size)

      j1 = JSON.parse(r1)
      expect(j1["action"]).to eq("B")

      # Insert ("I") 1
      h2 = c.stdout.gets
      r2 = c.stdout.gets

      expect(h2).to match(HEADER_REGEXP)
      h2_len = HEADER_REGEXP.match(h2)[:len].to_i
      expect(h2_len).to eq(r2.size)

      j2 = JSON.parse(r2)
      expect(j2["action"]).to eq("I")
      expect(j2["schema"]).to eq("public")
      expect(j2["table"]).to eq(table1)
      expect(j2["columns"]).to eq([
        {"name"=>"id", "type"=>"bigint", "value"=>1},
        {"name"=>"name", "type"=>"text", "value"=>"n1"},
        {"name"=>"extra", "type"=>"bytea", "value"=>nil}
      ])

      # Insert ("I") 2
      h3 = c.stdout.gets
      r3 = c.stdout.gets

      expect(h3).to match(HEADER_REGEXP)
      h3_len = HEADER_REGEXP.match(h3)[:len].to_i
      expect(h3_len).to eq(r3.size)

      j3 = JSON.parse(r3)
      expect(j3["action"]).to eq("I")
      expect(j3["schema"]).to eq("public")
      expect(j3["table"]).to eq(table1)
      expect(j3["columns"]).to eq([
        {"name"=>"id", "type"=>"bigint", "value"=>2},
        {"name"=>"name", "type"=>"text", "value"=>"n2"},
        {"name"=>"extra", "type"=>"bytea", "value"=>nil}
      ])

      # Commit ("C")
      h4 = c.stdout.gets
      r4 = c.stdout.gets

      expect(h4).to match(HEADER_REGEXP)
      h4_len = HEADER_REGEXP.match(h4)[:len].to_i
      expect(h4_len).to eq(r4.size)

      j4 = JSON.parse(r4)
      expect(j4["action"]).to eq("C")
    end
  end

  it "capture deletes" do
    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table1} (name) values ('n1'), ('n1')"
      pg_exec "delete from #{table1} where name = 'n1'"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Insert ("I") x2
      2.times do |i|
        h = c.stdout.gets
        r = c.stdout.gets
        j = JSON.parse(r)
        expect(j["action"]).to eq("I")
        expect(j["table"]).to eq(table1)
        expect(j["columns"]).to eq([
          {"name"=>"id", "type"=>"bigint", "value"=>(i + 1)},
          {"name"=>"name", "type"=>"text", "value"=>"n1"},
          {"name"=>"extra", "type"=>"bytea", "value"=>nil}
        ])
      end

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Delete ("D")
      2.times do |i|
        h = c.stdout.gets
        r = c.stdout.gets
        j = JSON.parse(r)
        expect(j["action"]).to eq("D")
        expect(j["schema"]).to eq("public")
        expect(j["table"]).to eq(table1)
        expect(j["identity"]).to eq([
          {"name"=>"id", "type"=>"bigint", "value"=>(i + 1)},
        ])
      end

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")
    end
  end

  it "capture updates" do
    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table1} (name) values ('n1'), ('n1')"
      pg_exec "update #{table1} set name = 'n2' where name = 'n1'"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Insert ("I") x2
      2.times do |i|
        h = c.stdout.gets
        r = c.stdout.gets
        j = JSON.parse(r)
        expect(j["action"]).to eq("I")
        expect(j["table"]).to eq(table1)
        expect(j["columns"]).to eq([
          {"name"=>"id", "type"=>"bigint", "value"=>(i + 1)},
          {"name"=>"name", "type"=>"text", "value"=>"n1"},
          {"name"=>"extra", "type"=>"bytea", "value"=>nil}
        ])
      end

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Update ("(")
      2.times do |i|
        h = c.stdout.gets
        r = c.stdout.gets
        j = JSON.parse(r)
        expect(j["action"]).to eq("U")
        expect(j["schema"]).to eq("public")
        expect(j["table"]).to eq(table1)
        expect(j["identity"]).to eq([
          {"name"=>"id", "type"=>"bigint", "value"=>(i + 1)},
        ])
        expect(j["columns"]).to eq([
          {"name"=>"id", "type"=>"bigint", "value"=>(i+1)},
          {"name"=>"name", "type"=>"text", "value"=>"n2"},
          {"name"=>"extra", "type"=>"bytea", "value"=>nil}
        ])
      end

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")
    end
  end

  it "capture date and time" do
    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table2} (c_date, c_timestamp, c_timestamptz) values ('2020-01-02', '2020-01-02 03:04:05.678', '2020-01-02 03:04:05.678 +0000')"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Insert ("I")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("I")
      expect(j["table"]).to eq(table2)
      expect(j["columns"]).to eq([
        {"name"=>"id", "type"=>"integer", "value"=>1},
        {"name"=>"c_date", "type"=>"date", "value"=>"2020-01-02"},
        {"name"=>"c_timestamp", "type"=>"timestamp without time zone", "value"=>"2020-01-02 03:04:05.678"},
        {"name"=>"c_timestamptz", "type"=>"timestamp with time zone", "value"=>"2020-01-02 03:04:05.678+00"}
      ])

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")
    end
  end

  it "resumes" do
    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table1} (name) values ('n1'), ('n2')"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Insert ("I")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("I")
    end

    # Restart without sending feedback

    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table1} (name) values ('n1'), ('n2')"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Insert ("I") x2
      2.times do |i|
        h = c.stdout.gets
        r = c.stdout.gets
        j = JSON.parse(r)
        expect(j["action"]).to eq("I")
        expect(j["table"]).to eq(table1)
        expect(j["columns"]).to eq([
          {"name"=>"id", "type"=>"bigint", "value"=>(i + 1)},
          {"name"=>"name", "type"=>"text", "value"=>"n#{i + 1}"},
          {"name"=>"extra", "type"=>"bytea", "value"=>nil}
        ])
      end

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")
    end
  end

  it "sends feedback" do
    lsn_before_feedback = nil
    lsn_after_feedback = nil

    stat = cmd(slot_name, "-N --wal2json2 -v") do |c|
      pg_exec "insert into #{table1} (name) values ('n1')"
      pg_exec "delete from #{table1} where name = 'n1'"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Insert ("I")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("I")

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")

      expect(h).to match(HEADER_REGEXP)
      lsn_before_ic = HEADER_REGEXP.match(h)[:lsn]

      # Send feedback
      r = pg_exec "select * from pg_replication_slots where slot_name = '#{slot_name}'"
      lsn_before_feedback = r[0]["confirmed_flush_lsn"]
      c.stdin.puts "F #{lsn_before_ic}"
      lsn_after_feedback = lsn_before_ic

      # Send quit
      c.stdin.puts "q"

      # Wait for completion
      c.stdout.read
    end

    # Exit code is SUCCESS
    expect(stat.exitstatus).to eq(0)

    # confirmed_flush_lsn should become the lsn sent
    r = pg_exec "select * from pg_replication_slots where slot_name = '#{slot_name}'"
    expect(lsn_before_feedback).not_to eq(lsn_after_feedback)
    expect(r[0]["confirmed_flush_lsn"]).to eq(lsn_after_feedback)

    cmd(slot_name, "-N --wal2json2") do |c|
      pg_exec "insert into #{table1} (name) values ('n1'), ('n2')"

      # Begin ("B")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("B")

      # Delete ("D")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("D")

      # Commit ("C")
      h = c.stdout.gets
      r = c.stdout.gets
      j = JSON.parse(r)
      expect(j["action"]).to eq("C")
    end
  end

  it "returns CMD_CLOSED when stdin is closed" do
    stat = cmd(slot_name, "-N --wal2json2 -v") do |c|
      h = c.stdin.close

      # Wait for completion
      c.stdout.read
    end

    # Exit code is CMD_CLOSED.
    expect(stat.exitstatus).to eq(4)
  end
end
