
[![Build Status](https://secure.travis-ci.org/mrkamel/cassandra_record.png?branch=master)](http://travis-ci.org/mrkamel/cassandra_record)

# CassandraRecord

CassandraRecord is a fun to use ORM for Cassandra with a chainable,
ActiveRecord like DSL for querying, inserting, updating and deleting records
plus built-in migration support. It is built on-top of the cassandra-driver
gem, using its built-in automated paging what is drastically reducing the
complexity of the code base.

## Install

Add this line to your application's Gemfile:

  gem 'cassandra_record'

And then execute:

  $ bundle

Or install it yourself as:

  $ gem install cassandra_record

## TODO

* YARD
* Rake task to migrate up/down
* README

## Semantic Versioning

CassandraRecord is using Semantic Versioning: [SemVer](http://semver.org/)

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

