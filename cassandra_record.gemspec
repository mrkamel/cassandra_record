# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassandra_record/version'

Gem::Specification.new do |spec|
  spec.name          = "cassandra_record"
  spec.version       = CassandraRecord::VERSION
  spec.authors       = ["Benjamin Vetter"]
  spec.email         = ["vetter@flakks.com"]
  spec.description   = %q{Powerful ORM for Cassandra}
  spec.summary       = %q{Easy to use ActiveRecord like ORM for Cassandra}
  spec.homepage      = "https://github.com/mrkamel/cassandra_record"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"

  spec.add_dependency "activemodel", ">= 3.0"
  spec.add_dependency "activesupport", ">= 3.0"
  spec.add_dependency "connection_pool"
  spec.add_dependency "cassandra-driver"
end

