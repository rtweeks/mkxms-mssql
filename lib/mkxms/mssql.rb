require "optparse"
require "ostruct"
require "pathname"
require "rexml/document"

module Mkxms
  module Mssql
    def self.parse_argv(argv = ARGV.dup)
      options = OpenStruct.new
      optparser = OptionParser.new do |flags|
        flags.banner = "Usage: #{File.basename($0)} [<option> [<option> ...]] DB_DESCRIPTION_FILE"
        flags.separator ''
        flags.separator 'Options:'
        
        options.schema_dir = Pathname.pwd
        flags.on('-o', '--outdir=SCHEMA_DIR', "Output in SCHEMA_DIR") do |schema_dir|
          options.schema_dir = Pathname(schema_dir).expand_path
        end
      end
      
      db_files = optparser.parse(argv)
      case db_files.length
      when ->(n) {n > 1}
        "Too many DB_DESCRIPTION_FILEs given"
      when ->(n) {n < 1}
        "No DB_DESCRIPTION_FILE given"
      end.tap {|msg| raise ProgramArgumentError.new(msg) if msg}
      
      return [db_files[0], options]
    end
    
    def self.generate_from(document, options)
      db_handler = DatabaseHandler.new(**(options.to_h))
      engine = Engine.new(document, db_handler)
      engine.run
      db_handler.create_source_files
    end
  end
end

require "mkxms/mssql/database_handler"
require "mkxms/mssql/engine"
require "mkxms/mssql/exceptions"
require "mkxms/mssql/version"

if __FILE__.eql? $0
  description_file, options = Mkxms::Mssql.parse_argv
  document = File.open(description_file, 'r') do |xml_file|
    REXML::Document.new(xml_file)
  end
  Mkxms::Mssql.generate_from(document, options)
end
