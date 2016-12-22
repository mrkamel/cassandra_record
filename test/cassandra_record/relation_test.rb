
require File.expand_path("../../test_helper", __FILE__)

class CassandraRecord::RelationTest < CassandraRecord::TestCase
  def test_all
    post1 = Post.create!(user: "user", domain: "domain", message: "message1")
    post2 = Post.create!(user: "user", domain: "domain", message: "message2")

    posts = Post.all.to_a

    assert_includes posts, post1
    assert_includes posts, post2
  end

  def test_where
    post1 = Post.create!(user: "user", domain: "domain1", message: "message1")
    post2 = Post.create!(user: "user", domain: "domain1", message: "message2")
    post3 = Post.create!(user: "user", domain: "domain2", message: "message1")

    posts = Post.where(user: "user").where(domain: "domain1").to_a

    assert_includes posts, post1
    assert_includes posts, post2
    refute_includes posts, post3
  end

  def test_where_with_array
    post1 = Post.create!(user: "user", domain: "domain1")
    post2 = Post.create!(user: "user", domain: "domain2")
    post3 = Post.create!(user: "user", domain: "domain3")

    posts = Post.where(user: "user").where(domain: ["domain1", "domain2"]).to_a

    assert_includes posts, post1
    assert_includes posts, post2
    refute_includes posts, post3
  end

  def test_where_with_range
    post1 = Post.create!(user: "user", domain: "domain1")
    post2 = Post.create!(user: "user", domain: "domain2")
    post3 = Post.create!(user: "user", domain: "domain3")

    posts = Post.where(user: "user").where(domain: "domain1" .. "domain2").to_a

    assert_includes posts, post1
    assert_includes posts, post2
    refute_includes posts, post3
  end

  def test_where_cql
    post1 = Post.create!(user: "user", domain: "domain1", message: "message1")
    post2 = Post.create!(user: "user", domain: "domain1", message: "message2")
    post3 = Post.create!(user: "user", domain: "domain2", message: "message1")

    posts = Post.where_cql("user = 'user'").where_cql("domain = :domain", domain: "domain1").to_a

    assert_includes posts, post1
    assert_includes posts, post2
    refute_includes posts, post3
  end

  def test_order
    post1 = Post.create!(user: "user", domain: "domain", timestamp: Time.now)
    post2 = Post.create!(user: "user", domain: "domain", timestamp: Time.now + 1.day)
    post3 = Post.create!(user: "user", domain: "domain", timestamp: Time.now + 2.days)

    assert_equal [post1, post2, post3], Post.where(user: "user", domain: "domain").order(id: :asc).to_a
    assert_equal [post3, post2, post1], Post.where(user: "user", domain: "domain").order(id: :desc).to_a
  end

  def test_limit
    Post.create!(user: "user", domain: "domain", timestamp: Time.now)
    Post.create!(user: "user", domain: "domain", timestamp: Time.now)
    Post.create!(user: "user", domain: "domain", timestamp: Time.now)
    Post.create!(user: "user", domain: "domain", timestamp: Time.now)

    assert_equal 2, Post.limit(2).find_each.count
    assert_equal 2, Post.where(user: "user", domain: "domain").limit(2).find_each.count

    assert_equal 3, Post.limit(3).find_each.count
    assert_equal 3, Post.where(user: "user", domain: "domain").limit(3).find_each.count
  end

  def test_first
    post1 = Post.create!(user: "user", domain: "domain", timestamp: Time.now - 1.day)
    post2 = Post.create!(user: "user", domain: "domain", timestamp: Time.now + 1.day)

    assert_equal post1, Post.where(user: "user", domain: "domain").order(id: :asc).first
    assert_equal post2, Post.where(user: "user", domain: "domain").order(id: :desc).first
  end

  def test_distinct
    Post.create! user: "user1", domain: "domain1", timestamp: Time.now
    Post.create! user: "user1", domain: "domain2", timestamp: Time.now
    Post.create! user: "user2", domain: "domain1", timestamp: Time.now

    assert_equal [{ "user" => "user1", "domain" => "domain1" }, { "user" => "user1", "domain" => "domain2" }, { "user" => "user2", "domain" => "domain1" }],
      Post.select(:user, :domain).distinct.find_each.to_a
  end

  def test_select
    Post.create! user: "user1", domain: "domain1", timestamp: Time.now
    Post.create! user: "user2", domain: "domain2", timestamp: Time.now

    assert_equal [{ "user" => "user1", "domain" => "domain1" }, { "user" => "user2", "domain" => "domain2" }], Post.select(:user, :domain).find_each.to_a
  end

  def test_find_each
    Post.create! user: "user1", domain: "domain1", timestamp: Time.now
    Post.create! user: "user2", domain: "domain2", timestamp: Time.now
    Post.create! user: "user3", domain: "domain3", timestamp: Time.now

    assert_equal ["user1", "user2", "user3"].to_set, Post.find_each(batch_size: 2).map(&:user).to_set
  end

  def test_find_in_batches
  end

  def test_count
  end

  def test_delete_all
  end

  def test_delete_in_batches
  end

  def test_to_a
  end
end

