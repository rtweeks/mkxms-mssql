# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mkxms/mssql/version'

Gem::Specification.new do |spec|
  spec.name          = "mkxms-mssql"
  spec.version       = Mkxms::Mssql::VERSION
  spec.authors       = ["Richard Weeks"]
  spec.email         = ["rtweeks21@gmail.com"]
  spec.summary       = %q{XMigra source files from MS-SQL database description.}
  spec.description   = %q{Build a complete set of XMigra source files from an XML document (as produced by the mssql-eyewkas.sql script) describing an MS-SQL database.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "xmigra", '~> 1.1'

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec", "~> 3.0"
end
