
require "cassandra"
require "connection_pool"
require "active_model"
require "active_support/all"

require "cassandra_record/version"
require "cassandra_record/base"
require "cassandra_record/relation"
require "cassandra_record/schema_migration"
require "cassandra_record/migration"

module CassandraRecord
  class RecordInvalid < StandardError; end
  class RecordNotPersisted < StandardError; end
  class UnknownType < StandardError; end
end

