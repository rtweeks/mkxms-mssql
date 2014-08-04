module Mkxms; end

module Mkxms::Mssql
  class DefaultConstraint
    def initialize(schema, table, column, name)
      @schema, @table, @column, @name = schema, table, column, name
      @expression = ''
    end
    
    attr_accessor :schema, :table, :column, :name, :expression
    
    def to_sql
      "ALTER TABLE #@schema.#@table ADD #{"CONSTRAINT #@name" if @name} DEFAULT #@expression FOR #@column;"
    end
    
    def qualified_table
      "#@schema.#@table"
    end
    
    def qualified_column
      "#@schema.#@table.#@column"
    end
    
    def qualified_name
      "#@schema.#@name" if @name
    end
  end

  class DefaultConstraintHandler
    def initialize(constraints, node)
      a = node.attributes
      @constraint = DefaultConstraint.new(
        a['schema'],
        a['table'],
        a['column'],
        a['name'],
      ).tap do |c|
        constraints << c
      end
    end
    
    def handle_text(text, parent_element)
      @constraint.expression << text
    end
  end
end
