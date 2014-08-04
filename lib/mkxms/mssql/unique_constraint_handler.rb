require 'mkxms/mssql/keylike_constraint_helper'

module Mkxms; end

module Mkxms::Mssql
  class UniqueConstraint < KeylikeConstraint
    SQL_CONSTRAINT_TYPE = 'UNIQUE'
    
    def sql_constraint_type
      SQL_CONSTRAINT_TYPE
    end
  end
  
  class UniqueConstraintHandler
    def initialize(constraints, node)
      a = node.attributes
      
      @uconst = UniqueConstraint.new(a).tap do |c|
        constraints << c
      end
    end
    
    def handle_column_element(parse)
      a = parse.node.attributes
      
      raise UnsupportedFeatureError.new("Unique constraints may not specify included columns (#{@uconst.qualified_table})") if a['included']
      @uconst.columns << IndexColumn.new(a['name'], a['desc'] ? :descending : :ascending)
    end
    
    # TODO: Handle partitioned unique constraints
  end
end
