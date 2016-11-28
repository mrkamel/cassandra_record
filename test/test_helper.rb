
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
  CREATE TABLE search_logs(
    date DATE,
    bucket INT,
    id TIMEUUID,
    query TEXT,
    username TEXT,
    PRIMARY KEY((date, bucket), id)
  )
EOF

class SearchLog < CassandraRecord::Base
  column :date, type: :date, key: true
  column :bucket, type: :int, key: true
  column :id, type: :uuid, key: true
  column :query, type: :text
  column :username, type: :text
end

class CassandraRecord::TestCase < MiniTest::Test
  def setup
    SearchLog.delete_all
  end
end

