
class CassandraRecord::Relation
  attr_accessor :target, :where_values, :where_cql_values, :order_values, :limit_value, :distinct_value, :select_values

  def initialize(target:)
    self.target = target
  end

  def all
    fresh
  end

  def where(hash = {})
    fresh.tap do |relation|
      relation.where_values = (relation.where_values || []) + [hash]
    end
  end

  def where_cql(string, args = {})
    fresh.tap do |relation|
      str = string

      args.each do |key, value|
        str.gsub!(":#{key}", target.quote_value(value))
      end

      relation.where_cql_values = (relation.where_cql_values || []) + [str]
    end
  end

  def order(hash = {})
    fresh.tap do |relation|
      relation.order_values = (relation.order_values || {}).merge(hash)
    end
  end

  def limit(n)
    fresh.tap do |relation|
      relation.limit_value = n
    end
  end

  def first(n = 1)
    result = limit(n).to_a

    return result.first if n == 1

    result
  end

  def distinct
    fresh.tap do |relation|
      relation.distinct_value = true
    end
  end

  def select(*columns)
    fresh.tap do |relation|
      relation.select_values = (relation.select_values || []) + columns
    end
  end

  def find_each(options = {})
    return enum_for(:find_each, options) unless block_given?

    find_in_batches options do |batch|
      batch.each do |record|
        yield record
      end
    end
  end

  def find_in_batches(batch_size: 1_000)
    return enum_for(:find_in_batches, batch_size: batch_size) unless block_given?

    each_page "SELECT #{select_clause} FROM #{target.table_name} #{where_clause} #{order_clause} #{limit_clause}", page_size: batch_size do |result|
      records = []

      result.each do |row|
        if select_values.present?
          records << row
        else
          records << load_record(row)
        end
      end

      yield(records) unless records.empty?
    end
  end

  def delete_all
    target.execute("DELETE FROM #{target.table_name} #{where_clause}")

    true
  end

  def delete_in_batches
    find_in_batches do |records|
      delete_statements = records.map do |record|
        where_clause = target.key_columns.map { |column, _| "#{column} = #{target.quote_value record.read_raw_attribute(column)}" }.join(" AND ")

        "DELETE FROM #{target.table_name} WHERE #{where_clause}"
      end

      target.execute_batch(delete_statements)
    end

    true
  end

  def count
    cql = "SELECT COUNT(*) FROM #{target.table_name} #{where_clause}"

    target.execute(cql).first["count"]
  end

  def to_a
    @records ||= find_each.to_a
  end

  private

  def load_record(row)
    target.new.tap do |record|
      record.persisted!

      row.each do |key, value|
        record.write_raw_attribute(key, value)
      end
    end
  end

  def fresh
    dup.tap do |relation|
      relation.instance_variable_set(:@records, nil)
    end
  end

  def each_page(cql, page_size:)
    result = target.execute(cql, page_size: page_size)

    while result
      yield result

      result = result.next_page
    end
  end

  def select_clause
    "#{distinct_value ? "DISTINCT" : ""} #{select_values.presence ? select_values.join(", ") : "*"}"
  end

  def where_clause
    return if where_values.blank? && where_cql_values.blank?

    constraints = []

    Array(where_values).each do |hash|
      hash.each do |column, value|
        if value.is_a?(Array) || value.is_a?(Range)
          constraints << "#{column} IN (#{value.to_a.map { |v| target.quote_value v }.join(", ")})"
        else
          constraints << "#{column} = #{target.quote_value value}"
        end
      end
    end

    constraints += Array(where_cql_values)

    "WHERE #{constraints.join(" AND ")}"
  end

  def order_clause
    "#{order_values.presence ? "ORDER BY #{order_values.map { |column, value| "#{column} #{value}" }.join(", ")}" : ""}"
  end

  def limit_clause
    "#{limit_value ? "LIMIT #{limit_value.to_i}" : ""}"
  end
end

