
require "minitest"
require "minitest/autorun"
require "cassandra_record"

cluster = Cassandra.cluster

connection = cluster.connect
connection.execute "DROP KEYSPACE IF EXISTS cassandra_record"
connection.execute "CREATE KEYSPACE cassandra_record WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"

CassandraRecord::Base.connection = cluster.connect("cassandra_record")
CassandraRecord::Base.logger.level = Logger::INFO

CassandraRecord::Base.connection.execute <<EOF
  CREATE TABLE test_logs(
    date DATE,
    bucket INT,
    id TIMEUUID,
    query TEXT,
    username TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY((date, bucket), id)
  )
EOF

class TestLog < CassandraRecord::Base
  column :date, :date, key: true
  column :bucket, :int, key: true
  column :id, :timeuuid, key: true
  column :username, :text
  column :timestamp, :timestamp

  validates_presence_of :timestamp

  def self.bucket_for(id)
    Digest::SHA1.hexdigest(id.to_s)[0].to_i(16) % 8
  end

  before_create do |record|
    record.id = generate_timeuuid(timestamp)

    record.date = record.id.to_date.strftime("%F")
    record.bucket = self.class.bucket_for(record.id)
  end
end

class CassandraRecord::TestCase < MiniTest::Test
  def setup
    TestLog.delete_all
  end

  def assert_difference(expressions, difference = 1, &block)
    callables = Array(expressions).map { |e| lambda { eval(e, block.binding) } }

    before = callables.map(&:call)

    res = yield

    Array(expressions).zip(callables).each_with_index do |(code, callable), i|
      assert_equal before[i] + difference, callable.call, "#{code.inspect} didn't change by #{difference}"
    end

    res
  end

  def assert_no_difference(expressions, &block)
    assert_difference(expressions, 0, &block)
  end

  def assert_present(object)
    assert object.present?, "should be present"
  end
end

