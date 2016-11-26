
class SchemaMigration < CassandraRecord
  def self.table_name
    "schema_migrations"
  end

  def self.create_table
    connection.execute "CREATE TABLE schema_migrations(version TEXT PRIMARY KEY)"
  end

  column :version, :text, key: true
end

