
class CassandraRecord::Migration
  def self.migration_file(path, version)
    Dir[File.join(path, "#{version}_*.rb")].first
  end 
  
  def self.migration_class(path, version)
    File.basename(migration_file(path, version), ".rb").gsub(/\A[0-9]+_/, "").camelcase.constantize
  end 

  def self.up(path, version)
    require migration_file(path, version)

    migration_class(path, version).new.up

    CassandraRecord::SchemaMigration.create!(version: version.to_s)
  end 

  def self.down(path, version)
    require migration_file(path, version)

    migration_class(path, version).new.down

    CassandraRecord::SchemaMigration.where(version: version.to_s).delete_all
  end 

  def self.migrate(path)
    migrated = CassandraRecord::SchemaMigration.all.to_a.map(&:version).to_set
    all = Dir[File.join(path, "*.rb")].map { |file| File.basename(file) }
    todo = all.select { |file| file =~ /\A[0-9]+_/ && !migrated.include?(file.to_i.to_s) }.sort_by(&:to_i)

    todo.each do |file|
      up path, file.to_i.to_s
    end 
  end 

  def execute(*args)
    CassandraRecord::Base.execute(*args)
  end 

  def up; end 
  def down; end 
end 

