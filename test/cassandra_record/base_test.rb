
require File.expand_path("../../test_helper", __FILE__)

class TestRecord < CassandraRecord::Base
  column :text, :text
  column :int, :int
  column :bigint, :bigint
  column :date, :date
  column :timestamp, :timestamp
  column :timeuuid, :timeuuid
  column :uuid, :uuid
end

class CassandraRecord::BaseTest < CassandraRecord::TestCase
  def test_new
    test_log = TestLog.new(timestamp: "2016-11-01 12:00:00", username: "username")

    assert_equal Time.parse("2016-11-01 12:00:00").utc.round(3), test_log.timestamp
    assert_equal "username", test_log.username
  end

  def test_assign
    test_log = TestLog.new
    test_log.assign(timestamp: "2016-11-01 12:00:00", username: "username")

    assert_equal Time.parse("2016-11-01 12:00:00").utc.round(3), test_log.timestamp
    assert_equal "username", test_log.username
  end

  def test_assign_persisted_key
    test_log = TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert test_log.persisted?

    assert_raises ArgumentError do
      test_log.assign(date: Date.parse("2016-11-02"))
    end
  end

  def test_attributes
    test_log = TestLog.new(timestamp: "2016-11-01 12:00:00", username: "username")

    assert_equal({ date: nil, bucket: nil, id: nil, username: "username", timestamp: Time.parse("2016-11-01 12:00:00").utc.round(3) }, test_log.attributes)
  end

  def test_casting
    test_record = TestRecord.new

    test_record.text = "text"
    assert_equal "text", test_record.text

    test_record.int = 1
    assert_equal 1, test_record.int
    test_record.int = "2"
    assert_equal 2, test_record.int

    test_record.bigint = 1
    assert_equal 1, test_record.bigint
    test_record.bigint = "2"
    assert_equal 2, test_record.bigint

    test_record.date = Date.new(2016, 11, 1)
    assert_equal Date.new(2016, 11, 1), test_record.date
    test_record.date = "2016-11-02"
    assert_equal Date.new(2016, 11, 2), test_record.date

    test_record.timestamp = Time.parse("2016-11-01 12:00:00")
    assert_equal Time.parse("2016-11-01 12:00:00").utc.round(3), test_record.timestamp
    test_record.timestamp = "2016-11-02 12:00:00"
    assert_equal Time.parse("2016-11-02 12:00:00").utc.round(3), test_record.timestamp
    test_record.timestamp = Time.parse("2016-11-03 12:00:00").to_i
    assert_equal Time.parse("2016-11-03 12:00:00").utc.round(3), test_record.timestamp

    test_record.timeuuid = Cassandra::TimeUuid.new("1ce29e82-b2ea-11e6-88fa-2971245f69e1")
    assert_equal Cassandra::TimeUuid.new("1ce29e82-b2ea-11e6-88fa-2971245f69e1"), test_record.timeuuid
    test_record.timeuuid = "1ce29e82-b2ea-11e6-88fa-2971245f69e2"
    assert_equal Cassandra::TimeUuid.new("1ce29e82-b2ea-11e6-88fa-2971245f69e2"), test_record.timeuuid
    test_record.timeuuid = 38395057947756324226486198980982041059
    assert_equal Cassandra::TimeUuid.new(38395057947756324226486198980982041059), test_record.timeuuid

    test_record.uuid = Cassandra::Uuid.new("b9af7b9b-9317-43b3-922e-fe303f5942c1")
    assert_equal Cassandra::Uuid.new("b9af7b9b-9317-43b3-922e-fe303f5942c1"), test_record.uuid
    test_record.uuid = "b9af7b9b-9317-43b3-922e-fe303f5942c2"
    assert_equal Cassandra::Uuid.new("b9af7b9b-9317-43b3-922e-fe303f5942c2"), test_record.uuid
    test_record.uuid = 13466612472233423808722080080896418394
    assert_equal Cassandra::Uuid.new(13466612472233423808722080080896418394), test_record.uuid
  end

  def test_save
    test_log = TestLog.new

    assert_no_difference "TestLog.count" do
      refute test_log.save
    end

    assert_includes test_log.errors[:timestamp], "can't be blank"

    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"), username: "username")

    assert_difference "TestLog.count" do
      assert test_log.save
    end

    assert test_log.persisted?

    assert_equal Date.parse("2016-11-01"), test_log.date
    assert_equal "username", test_log.username
    assert_present test_log.bucket
    assert_present test_log.id
  end

  def test_save!
    test_log = TestLog.new

    assert_no_difference "TestLog.count" do
      assert_raises CassandraRecord::RecordInvalid do
        test_log.save!
      end
    end

    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert_difference "TestLog.count" do
      assert test_log.save
    end
  end

  def test_create
    test_log = nil

    assert_no_difference "TestLog.count" do
      test_log = TestLog.create
    end

    refute test_log.persisted?
    assert_includes test_log.errors[:timestamp], "can't be blank"

    assert_difference "TestLog.count" do
      test_log = TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00"), username: "username")
    end

    assert test_log.persisted?

    assert_equal Date.parse("2016-11-01"), test_log.date
    assert_equal "username", test_log.username
    assert_present test_log.bucket
    assert_present test_log.id
  end

  def test_create!
    assert_no_difference "TestLog.count" do
      assert_raises CassandraRecord::RecordInvalid do
        TestLog.create!
      end
    end

    assert_difference "TestLog.count" do
      TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00"))
    end
  end

  def test_update
    test_log = TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert test_log.persisted?
    assert_nil test_log.username

    refute test_log.update(timestamp: nil)
    assert_includes test_log.errors[:timestamp], "can't be blank"

    assert test_log.update(username: "username", timestamp: Time.parse("2016-11-02 12:00:00"))

    test_log = TestLog.where(date: test_log.date, bucket: test_log.bucket, id: test_log.id).first

    assert_equal "username", test_log.username
    assert_equal Time.parse("2016-11-02 12:00:00").utc.round(3), test_log.timestamp
  end

  def test_update!
    test_log = TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert test_log.persisted?

    assert_raises CassandraRecord::RecordInvalid do
      test_log.update!(timestamp: nil)
    end

    assert test_log.update!(username: "username", timestamp: Time.parse("2016-11-02 12:00:00"))
  end

  def test_persisted?
    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"))

    refute test_log.persisted?
    assert test_log.save
    assert test_log.persisted?
  end

  def test_new_record?
    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert test_log.new_record?
    assert test_log.save
    refute test_log.new_record?
  end

  def test_delete
    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert_difference "TestLog.count" do
      assert test_log.save
    end

    assert_difference "TestLog.count", -1 do
      test_log.delete
    end
  end

  def test_destroy
    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"))

    assert_difference "TestLog.count" do
      assert test_log.save
    end

    assert_difference "TestLog.count", -1 do
      test_log.destroy
    end

    assert test_log.destroyed?
  end

  def test_destroyed?
    test_log = TestLog.new(timestamp: Time.parse("2016-11-01 12:00:00"))

    refute test_log.destroyed?
    assert test_log.save
    refute test_log.destroyed?
    assert test_log.destroy
    assert test_log.destroyed?
  end

  def test_table_name
    assert_equal "test_logs", TestLog.table_name
  end

  def test_truncate_table
    TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00"))
    TestLog.create!(timestamp: Time.parse("2016-11-02 12:00:00"))

    TestLog.truncate_table

    assert_equal 0, TestLog.count
  end

  def test_execute
    records = [
      TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00")),
      TestLog.create!(timestamp: Time.parse("2016-11-02 12:00:00"))
    ]

    assert_equal records.map(&:id).to_set, TestLog.execute("SELECT * FROM test_logs", consistency: :all).map { |row| row["id"] }.to_set
  end

  def test_execute_batch
    records = [
      TestLog.create!(timestamp: Time.parse("2016-11-01 12:00:00")),
      TestLog.create!(timestamp: Time.parse("2016-11-02 12:00:00"))
    ]

    batch = [
      "DELETE FROM test_logs WHERE date = '#{records[0].date.strftime("%F")}' AND bucket = #{records[0].bucket} AND id = #{records[0].id}",
      "DELETE FROM test_logs WHERE date = '#{records[1].date.strftime("%F")}' AND bucket = #{records[1].bucket} AND id = #{records[1].id}"
    ]

    assert_difference "TestLog.count", -2 do
      TestLog.execute_batch(batch, consistency: :all)
    end
  end

  def test_callbacks
    temp_log = Class.new(TestLog) do
      def self.table_name
        "test_logs"
      end

      def called_callbacks
        @called_callbacks ||= []
      end

      def reset_called_callbacks
        @called_callbacks = []
      end

      before_validation { called_callbacks << :before_validation }
      after_validation { called_callbacks << :after_validation }
      before_save { called_callbacks << :before_save }
      after_save { called_callbacks << :after_save}
      before_create { called_callbacks << :before_create }
      after_create { called_callbacks << :after_create }
      before_update { called_callbacks << :before_update }
      after_update { called_callbacks << :after_update }
      before_destroy { called_callbacks << :before_destroy }
      after_destroy { called_callbacks << :after_destroy }
    end

    record = temp_log.create!(timestamp: Time.now)

    assert_equal [:before_validation, :after_validation, :before_save, :before_create, :after_create, :after_save], record.called_callbacks

    record = temp_log.create!(timestamp: Time.now)
    record.reset_called_callbacks
    record.save

    assert_equal [:before_validation, :after_validation, :before_save, :before_update, :after_update, :after_save], record.called_callbacks

    record = temp_log.create!(timestamp: Time.now)
    record.reset_called_callbacks
    record.destroy

    assert_equal [:before_destroy, :after_destroy], record.called_callbacks
  end

  def test_validate!
  end

  def test_save_batch
  end

  def test_destroy_batch
  end

  def test_delete_batch
  end

  def test_dirty
  end
end

