require 'mkxms/mssql/keylike_constraint_helper'

module Mkxms::Mssql
  class PrimaryKey < Mkxms::Mssql::KeylikeConstraint
    SQL_CONSTRAINT_TYPE = 'PRIMARY KEY'
    
    def sql_constraint_type
      SQL_CONSTRAINT_TYPE
    end
  end

  class PrimaryKeyHandler
    include PropertyHandler::ElementHandler
    
    def initialize(constraints, node)
      a = node.attributes
      
      @pkey = PrimaryKey.new(a).tap do |c|
        constraints << c
      end
    end
    
    def extended_properties
      @pkey.extended_properties
    end
    
    def handle_column_element(parse)
      a = parse.node.attributes
      
      raise UnsupportedFeatureError.new("Primary keys may not specify included columns (#{@pkey.qualified_table})") if a['included']
      @pkey.columns << IndexColumn.new(a['name'], a['desc'] ? :descending : :ascending)
    end
    
    # TODO: Handle partitioned primary keys
  end
end
