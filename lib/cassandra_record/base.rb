
class CassandraRecord::Base
  include ActiveModel::Dirty
  include ActiveModel::Validations
  include Hooks

  class_attribute :connection_pool

  class_attribute :logger
  self.logger = Logger.new(STDOUT)
  self.logger.level = Logger::INFO

  class_attribute :columns
  self.columns = {}

  define_hooks :before_validation, :after_validation, :before_create, :after_create, :before_update, :after_update, :before_save, :after_save, :before_destroy, :after_destroy

  def initialize(attributes = {})
    @persisted = false
    @destroyed = false

    assign(attributes)
  end

  def assign(attributes = {})
    attributes.each do |column, value|
      send(:"#{column}=", value)
    end
  end

  def attributes
    columns.each_with_object({}) do |(name, _), hash|
      hash[name] = read_raw_attribute(name)
    end
  end

  def read_raw_attribute(attribute)
    return nil unless instance_variable_defined?(:"@#{attribute}")

    instance_variable_get(:"@#{attribute}")
  end

  def write_raw_attribute(attribute, value)
    instance_variable_set(:"@#{attribute}", value)
  end

  def self.create!(attributes = {})
    new(attributes).tap(&:save!)
  end

  def self.create(attributes = {})
    new(attributes).tap(&:save)
  end

  def save!
    validate!

    _save
  end
    
  def save
    return false unless valid?

    _save
  end

  def valid?
    run_hook :before_validation

    retval = super

    run_hook :after_validation

    retval
  end

  def validate!
    valid? || raise(CassandraRecord::RecordInvalid, errors.to_a.join(", "))
  end

  def persisted?
    !! @persisted
  end

  def persisted!
    @persisted = true
  end

  def new_record?
    !persisted?
  end

  def destroyed?
    !! @destroyed
  end

  def destroyed!
    @destroyed = true
  end

  def update(attributes = {})
    assign(attributes)

    save
  end

  def update!(attributes = {})
    assign(attributes)

    save!
  end

  def destroy
    raise CassandraRecord::RecordNotPersisted unless persisted?

    run_hook :before_destroy

    delete

    destroyed!

    run_hook :after_destroy

    true
  end

  def delete
    raise CassandraRecord::RecordNotPersisted unless persisted?

    self.class.execute(delete_record_statement)

    true
  end

  def self.table_name
    name.tableize
  end

  def self.key_columns
    columns.select { |_, options| options[:key] }
  end
    
  def self.column(name, type, key: false)
    self.columns = columns.merge(name => { type: type, key: key })

    define_attribute_methods name

    define_method name do
      read_raw_attribute(name)
    end

    define_method :"#{name}=" do |value|
      raise(ArgumentError, "Can't update key '#{name}' for persisted records") if persisted? && self.class.columns[name][:key]

      send :"#{name}_will_change!" unless read_raw_attribute(name) == value

      write_raw_attribute(name, self.class.cast_value(value, type))
    end
  end

  def self.relation
    CassandraRecord::Relation.new(target: self)
  end

  class << self
    delegate :all, :where, :count, :limit, :first, :order, :distinct, :select, :find_each, :find_in_batches, :delete_in_batches, to: :relation
  end

  def self.cast_value(value, type)
    return nil if value.nil?

    case type
      when :text
        value.to_s
      when :int, :bigint
        Integer(value)
      when :boolean
        return true if [1, "1", "true", true].include?(value)
        return false if [0, "0", "false", false].include?(value)
        raise ArgumentError, "Can't cast '#{value}' to #{type}"
      when :date
        if value.is_a?(String) then Date.parse(value)
        elsif value.respond_to?(:to_date) then value.to_date
        else raise(ArgumentError, "Can't cast '#{value}' to #{type}")
        end
      when :timestamp
        if value.is_a?(String) then Time.parse(value)
        elsif value.respond_to?(:to_time) then value.to_time
        elsif value.is_a?(Numeric) then Time.at(value)
        else raise(ArgumentError, "Can't cast '#{value}' to #{type}")
        end.utc.round(3)
      when :timeuuid
        return value if value.is_a?(Cassandra::TimeUuid)
        return Cassandra::TimeUuid.new(value) if value.is_a?(String) || value.is_a?(Integer)
        raise ArgumentError, "Can't cast '#{value}' to #{type}"
      when :uuid
        return value if value.is_a?(Cassandra::Uuid)
        return Cassandra::Uuid.new(value) if value.is_a?(String) || value.is_a?(Integer)
        raise ArgumentError, "Can't cast '#{value}' to #{type}"
      else
        raise CassandraRecord::UnknownType, "Unknown type #{type}"
    end
  end

  def self.quote_value(value)
    case value
      when Time, ActiveSupport::TimeWithZone
        (value.to_r * 1000).round.to_s
      when DateTime
        quote_value(value.utc.to_time)
      when Date
        quote_value(value.strftime("%Y-%m-%d"))
      when Numeric, true, false, Cassandra::Uuid
        value.to_s
      else
        quote_string(value.to_s)
    end
  end

  def self.quote_string(string)
    "'#{string.gsub("'", "''")}'"
  end 

  def self.truncate_table
    execute "TRUNCATE TABLE #{table_name}"
  end

  def self.execute(statement, options = {})
    logger.debug(statement)

    connection_pool.with do |connection|
      connection.execute(statement, options)
    end
  end

  def self.execute_batch(statements, options = {})
    statements.each do |statement|
      logger.debug(statement)
    end

    connection_pool.with do |connection|
      batch = connection.batch

      statements.each do |statement|
        batch.add(statement)
      end

      connection.execute(batch, options)
    end
  end

  private

  def _save
    if persisted?
      run_hook :before_save

      update_record

      run_hook :after_save
    else
      run_hook :before_save

      create_record
      persisted!

      run_hook :after_save
    end

    changes_applied

    true
  end

  def self._save_batch(records, options = {})
    records.each do |record|
      record.run_hook :before_save
      record.run_hook(record.persisted? ? :before_update : :before_create)
    end

    statements = records.flat_map do |record|
      record.send(record.persisted? ? :update_record_statements : :create_record_statement)
    end

    execute_batch(statements, options) unless statements.empty?

    persistence = records.map(&:persisted?)

    records.each(&:persisted!)

    records.each_with_index do |record, index|
      record.run_hook(persistence[index] ? :after_update : :after_create)
      record.run_hook :after_save
    end

    records.each do |record|
      record.send :changes_applied
    end

    true
  end

  def create_record
    run_hook :before_create

    self.class.execute(create_record_statement)

    run_hook :after_create
  end

  def create_record_statement
    columns_clause = changes.keys.join(", ")
    values_clause = changes.values.map(&:last).map { |value| self.class.quote_value value }.join(", ")

    "INSERT INTO #{self.class.table_name}(#{columns_clause}) VALUES(#{values_clause})"
  end

  def update_record
    run_hook :before_update

    self.class.execute_batch(update_record_statements) unless changes.empty?

    run_hook :after_update
  end

  def update_record_statements
    nils = changes.select { |_, (__, new_value)| new_value.nil? }
    objs = changes.reject { |_, (__, new_value)| new_value.nil? }

    statements = []

    if nils.present?
      statements << "DELETE #{nils.keys.join(", ")} FROM #{self.class.table_name} #{where_key_clause}"
    end

    if objs.present?
      update_clause = objs.map { |column, (_, new_value)| "#{column} = #{self.class.quote_value new_value}" }.join(", ")

      statements << "UPDATE #{self.class.table_name} SET #{update_clause} #{where_key_clause}"
    end

    statements
  end

  def delete_record_statement
    "DELETE FROM #{self.class.table_name} #{where_key_clause}"
  end

  def where_key_clause
    "WHERE #{self.class.key_columns.map { |column, _| "#{column} = #{self.class.quote_value read_raw_attribute(column)}" }.join(" AND ")}"
  end

  def generate_uuid
    @uuid_generator ||= Cassandra::Uuid::Generator.new
    @uuid_generator.uuid
  end

  def generate_timeuuid(time)
    @timeuuid_generator ||= Cassandra::TimeUuid::Generator.new
    @timeuuid_generator.at(time)
  end
end

