
class CassandraRecord::SchemaMigration < CassandraRecord::Base
  def self.table_name
    "schema_migrations"
  end

  def self.create_table
    execute "CREATE TABLE schema_migrations(version TEXT PRIMARY KEY)"
  end

  column :version, :text, partition_key: true
end

