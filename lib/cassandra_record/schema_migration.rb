
class CassandraRecord::SchemaMigration < CassandraRecord::Base
  def self.table_name
    "schema_migrations"
  end

  def self.create_table(if_not_exists: false)
    execute "CREATE TABLE #{"IF NOT EXISTS" if if_not_exists} schema_migrations(version TEXT PRIMARY KEY)"
  end

  column :version, :text, partition_key: true
end

