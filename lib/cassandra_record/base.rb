
class CassandraRecord::Base
  include ActiveModel::Dirty
  include ActiveModel::Validations
  include ActiveModel::Validations::Callbacks
  extend ActiveModel::Callbacks

  class RecordInvalid < StandardError; end
  class RecordNotPersisted < StandardError; end
  class RecordAlreadyPersisted < StandardError; end

  class_attribute :connection

  class_attribute :logger
  self.logger = Logger.new(STDOUT)

  class_attribute :columns
  self.columns = {}

  define_model_callbacks :create, :update, :save, :destroy

  def initialize(attributes = {})
    assign(attributes)
  end

  def assign(attributes = {})
    attributes.each do |column, value|
      send(:"#{column}=", value) if columns[column.to_sym]
    end
  end

  def attributes
    columns.each_with_object({}) do |(name, args), hash|
      hash[name] = instance_variable_get(:"@#{name}")
    end
  end

  def self.create!(attributes = {})
    new(attributes).tap(&:save!)
  end

  def self.create(attributes = {})
    new(attributes).tap(&:save)
  end

  def save!
    valid?(persisted? ? :update : :create) || raise(RecordInvalid, errors.to_a.join(", "))

    save
  end
    
  def save
    if persisted?
      return false unless valid?(:update)

      update_record
    else
      return false unless valid?(:create)

      create_record
    end
  end

  def persisted?
    !! @persisted
  end

  def new_record?
    !persisted?
  end

  def destroyed?
    !! @destroyed
  end

  def update(attributes = {})
    assign(attributes)

    save
  end

  def destroy
    raise RecordNotPersisted unless persisted?

    run_callbacks :destroy do
      cql = "DELETE FROM #{self.class.table_name} #{where_key_clause}"

      self.class.logger.debug(cql)
      self.class.connection.execute(cql)

      @destroyed = true
    end

    true
  end

  def self.table_name
    name.tableize
  end

  def self.key_columns
    columns.select { |_, options| options[:key] }
  end
    
  def self.column(name, type, key: false)
    columns[name] = { type: type, key: key }

    define_attribute_methods name

    define_method name do
      instance_variable_get :"@#{name}"
    end

    define_method :"#{name}=" do |value|
      raise(ArgumentError, "Can't update key '#{name}' for persisted records") if persisted? && self.class.columns[name][:key]

      send :"#{name}_will_change!" unless instance_variable_get(:"@#{name}") == value

      instance_variable_set :"@#{name}", self.class.cast_value(value, type)
    end
  end

  def self.relation
    Relation.new(target: self)
  end

  class << self
    delegate :all, :where, :count, :limit, :first, :order, :distinct, :select, :find_each, :find_in_batches, to: :relation
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
        raise UnknownType, "Unknown type #{type}"
    end
  end

  def self.quote_value(value)
    case value
      when Time, ActiveSupport::TimeWithZone
        (value.to_r * 1000).round.to_s
      when DateTime
        quote_value(value.utc.to_time)
      when Date
        quote_value(Time.gm(value.year, value.month, value.day))
      when Numeric, true, false, Cassandra::Uuid
        value.to_s
      else
        quote_string(value.to_s)
    end
  end

  def self.quote_string(string)
    "'#{string.gsub("'", "''")}'"
  end 

  private

  def create_record
    raise RecordAlreadyPersisted if persisted?

    run_callbacks :save do
      run_callbacks :create do
        if changes.present?
          columns_clause = changes.keys.join(", ")
          values_clause = changes.values.map(&:last).map { |value| self.class.quote_value value }.join(", ")

          cql = "INSERT INTO #{self.class.table_name}(#{columns_clause}) VALUES(#{values_clause})"

          self.class.logger.debug(cql)
          self.class.connection.execute(cql)
        end
      end
    end

    changes_applied

    true
  end

  def update_record
    raise RecordNotPersisted unless persisted?

    run_callbacks :save do
      run_callbacks :update do
        if changes.present?
          nils = changes.select { |_, (__, new_value)| new_value.nil? }
          objs = changes.reject { |_, (__, new_value)| new_value.nil? }

          cqls = []

          if nils.present?
            cqls << "DELETE #{nils.keys.join(", ")} FROM #{self.class.table_name} #{where_key_clause}"
          end

          if objs.present?
            update_clause = objs.map { |column, (_, new_value)| "#{column} = #{self.class.quote_value new_value}" }.join(", ")

            cqls << "UPDATE #{self.class.table_name} SET #{update_clause} #{where_key_clause}"
          end

          self.class.logger.debug ["BEGIN BATCH", cqls, "END_BATCH"].flatten.join("\n")

          batch = connection.batch

          cqls.each do |cql|
            batch.add cql
          end

          self.class.connection.execute(batch)
        end
      end
    end

    changes_applied

    true
  end

  def where_key_clause
    "WHERE #{self.class.key_columns.map { |column, _| "#{column} = #{self.class.quote_value instance_variable_get(:"@#{column}")}" }.join(" AND ")}"
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

