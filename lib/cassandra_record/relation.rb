
class Relation
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
          record = target.new
          record.instance_variable_set(:"@persisted", true)

          row.each do |key, value|
            record.instance_variable_set(:"@#{key}", value)
          end

          records << record
        end
      end

      yield records
    end
  end

  def count
    cql = "SELECT COUNT(*) FROM #{target.table_name} #{where_clause}"

    target.logger.debug(cql)
    target.connection.execute(cql).first["count"]
  end

  def fresh
    dup.tap do |relation|
      relation.instance_variable_set(:@response, nil)
    end
  end

  def to_a
    @records ||= find_each.to_a
  end

  private

  def each_page(cql, page_size:)
    page = 0

    target.logger.debug("#{cql} [#{page * page_size}, #{(page + 1) * page_size}]")

    result = target.connection.execute(cql, page_size: page_size)

    while result
      yield result

      page += 1

      target.logger.debug("#{cql} [#{page * page_size}, #{(page + 1) * page_size}]")

      result = result.next_page
    end
  end

  def select_clause
    "#{distinct_value ? "DISTINCT" : ""} #{select_values.presence ? select_values.join(", ") : "*"}"
  end

  def where_clause
    return if where_values.blank? && where_cql_values.blank?

    constraints = []

    where_values.each do |hash|
      hash.each do |column, value|
        if value.is_a?(Array) || value.is_a?(Range)
          constraints << "#{column} IN (#{value.to_a.map { |v| target.quote_value v }.join(", ")})"
        else
          constraints << "#{column} = #{target.quote_value value}"
        end
      end
    end if where_values.present?

    constraints += where_cql_values if where_cql_values.present?

    "WHERE #{constraints.join(" AND ")}"
  end

  def order_clause
    "#{order_values.presence ? "ORDER BY #{order_values.map { |column, value| "#{column} #{value}" }.join(", ")}" : ""}"
  end

  def limit_clause
    "#{limit_value ? "LIMIT #{limit_value.to_i}" : ""}"
  end
end

