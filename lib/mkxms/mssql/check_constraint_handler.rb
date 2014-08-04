require 'mkxms/mssql/property_handler'

module Mkxms; end

module Mkxms::Mssql
  class CheckConstraint
    include ExtendedProperties, Property::Hosting
    
    def initialize(schema, table, name, enabled: true, when_replicated: true)
      @schema = schema
      @table = table
      @name = name
      @enabled = enabled
      @when_replicated = when_replicated
      @expression = ''
    end
    
    attr_accessor :schema, :table, :name, :enabled, :when_replicated
    attr_reader :expression
    
    def to_sql
      "ALTER TABLE #@schema.#@table ADD%s CHECK%s #@expression;%s" % [
        @name ? " CONSTRAINT #@name" : '',
        @when_replicated ? '' : ' NOT FOR REPLICATION',
        @enabled ? '' : "\nALTER TABLE #@schema.#@table NOCHECK CONSTRAINT #@name;"
      ] + (name ? extended_properties_sql.joined_on_new_lines : '')
    end
    
    def qualified_table
      "#@schema.#@table"
    end
    
    def qualified_name
      "#@schema.#@name" if @name
    end
    
    def property_subject_identifiers
      ['SCHEMA', schema, 'TABLE', table, 'CONSTRAINT', name].map {|s| Utils::unquoted_name(s)}
    end
  end

  class CheckConstraintHandler
    include PropertyHandler::ElementHandler
    
    def initialize(constraints, node)
      a = node.attributes
      
      @check = CheckConstraint.new(a['schema'], a['table'], a['name'],
                                   enabled: !a['disabled'], when_replicated: !a['not-for-replication'])
    end
    
    def handle_text(text, parent_element)
      @check.expression << text
    end
  end
end
