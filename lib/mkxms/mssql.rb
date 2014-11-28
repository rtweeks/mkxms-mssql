require "optparse"
require "ostruct"
require "pathname"
require "rexml/document"

module Mkxms
  module Mssql
    def self.parse_argv(argv = ARGV.dup)
      options = OpenStruct.new
      optparser = OptionParser.new do |flags|
        flags.banner = "Usage: #{File.basename($0)} [<option> [<option> ...]] [DB_DESCRIPTION_FILE]"
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
      end.tap {|msg| raise ProgramArgumentError.new(msg) if msg}
      
      return [db_files[0], options]
    end
    
    def self.generate_from(document, options)
      db_handler = DatabaseHandler.new(**(options.to_h))
      engine = Engine.new(document, db_handler)
      engine.run
      db_handler.create_source_files
    end
    
    def self.with_db_description_io(file_path, &blk)
      if file_path
        File.open(file_path, 'r', &blk)
      else
        blk.call($stdin)
      end
    end
    
    def self.run_program(argv = ARGV.dup)
      description_file, options = parse_argv(argv)
      document = with_db_description_io(description_file) do |xml_file|
        REXML::Document.new(xml_file)
      end
      generate_from(document, options)
    end
  end
end

require "mkxms/mssql/database_handler"
require "mkxms/mssql/engine"
require "mkxms/mssql/exceptions"
require "mkxms/mssql/version"

if __FILE__.eql? $0
  Mkxms::Mssql.run_program
end
