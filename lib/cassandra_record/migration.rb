
class Migration
  def self.migration_file(path, version)
    Dir[File.join(path, "#{version}_*.rb")].first
  end 
  
  def self.migration_class(path, version)
    File.basename(migration_file(path, version), ".rb").gsub(/\A[0-9]+_/, "").camelcase.constantize
  end 

  def self.up(path, version)
    require migration_file(path, version)

    migration_class(path, version).new.up

    SchemaMigration.create!(version: version.to_s)
  end 

  def self.down(path, version)
    require migration_file(path, version)

    migration_class(path, version).new.down

    SchemaMigration.where(version: version.to_s).destroy
  end 

  def self.migrate(path)
    migrated = SchemaMigration.all.to_a.map(&:version).to_set
    all = Dir[File.join(path, "*.rb")].map { |file| File.basename(file) }
    todo = all.select { |file| file =~ /\A[0-9]+_/ && !migrated.include?(file.to_i.to_s) }

    todo.each do |file|
      up path, file.to_i.to_s
    end 
  end 

  def execute(cql, *args)
    CassandraRecord.logger.debug(cql)
    CassandraRecord.connection.execute(cql, *args)
  end 

  def up; end 
  def down; end 
end 

