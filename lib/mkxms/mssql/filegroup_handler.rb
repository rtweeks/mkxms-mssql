require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Filegroup
    include ExtendedProperties
    
    def initialize(default: false, read_only: false)
      @default = default
      @read_only = read_only
    end
    
    def default?
      return @default
    end
    
    def read_only?
      return @read_only
    end
  end

  class FilegroupHandler
    include PropertyHandler::ElementHandler
    
    def initialize(filegroups, node)
      group_options = Hash[
        %w[default read-only].map do |a|
          [Utils.code_sym_for(a), node.attributes.has_key?(a)]
        end
      ]
      @filegroup = Filegroup.new(**group_options).tap do |fg|
        filegroups << fg
      end
      @files = []
    end
    
    def extended_properties
      @filegroup.extended_properties
    end
    
    def handle_file_element(parse)
      parse.context = DatabaseFile.new(@files, parse.node)
    end
  end

  class DatabaseFile
    include ExtendedProperties, PropertyHandler::ElementHandler
    
    def initialize(files, node)
      @properties = Hash[
        node.attributes.each_pair.map do |k, v|
          [Utils.code_sym_for(k), (k == v ? true : v)]
        end
      ]
    end
    
    def name
      @properties[:name]
    end
    
    def offline?
      @properties[:offline]
    end
    
    def max_size_kb
      value = @properties[:max_size]
      return :available_space if value == 'available'
      return value.to_i
    end
    
    def growth
      @properties[:growth].to_i
    end
    
    def grow_by_fraction?
      @properties[:growth_units] == 'percent'
    end
  end
end
