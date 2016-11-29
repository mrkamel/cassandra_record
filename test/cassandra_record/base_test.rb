
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

  def test_persisted
  end

  def test_new_record?
  end

  def test_delete
  end

  def test_destroy
  end

  def test_destroyed?
  end

  def test_table_name
  end

  def test_execute
  end

  def test_execute_batch
  end
end

