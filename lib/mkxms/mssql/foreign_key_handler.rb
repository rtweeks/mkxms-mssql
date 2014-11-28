require 'mkxms/mssql/property_handler'

module Mkxms; end

module Mkxms::Mssql
  class ForeignKey
    include ExtendedProperties, Property::Hosting
    
    def self.generated_name
      "XMigra_unnamed_foreign_key_constraint_#{@anon_counter = (@anon_counter || 0) + 1}"
    end
    
    def initialize(schema, table, name, on_delete: 'NO ACTION', on_update: 'NO ACTION', enabled: true)
      @schema = schema
      @table = table
      @delete_reconciliation = on_delete
      @update_reconciliation = on_update
      @enabled = enabled
      @name = name || self.class.generated_name
      @is_unnamed = !name
      @links = []
    end
    
    attr_accessor :schema, :table, :name
    attr_accessor :delete_reconciliation, :update_reconciliation, :enabled
    attr_accessor :references
    attr_reader :links # Array of elements like [column_in_referrer, column_in_referent]
    
    def to_sql
      "ALTER TABLE #{qualified_table} ADD CONSTRAINT #@name FOREIGN KEY (%s) REFERENCES #{@references[0]}.#{@references[1]} (%s)%s%s;" % [
        @links.map {|e| e[0]}.join(', '),
        @links.map {|e| e[1]}.join(', '),
        (" ON DELETE #@delete_reconciliation" if @delete_reconciliation),
        (" ON UPDATE #@update_reconciliation" if @update_reconciliation),
      ] + (
        @enabled ? '' : "\nALTER TABLE #{qualified_table} NOCHECK CONSTRAINT #@name;"
      ) + extended_properties_sql.joined_on_new_lines
    end
    
    def qualified_table
      "#@schema.#@table"
    end
    
    def qualified_name
      "#@schema.#@name" if @name
    end
    
    def property_subject_identifiers
      ['SCHEMA', schema, 'TABLE', table, 'CONSTRAINT', name].map {|n| Utils.unquoted_name(n)}
    end
    
    def unnamed?
      @is_unnamed
    end
  end
  
  class ForeignKeyHandler
    include PropertyHandler::ElementHandler
    
    def initialize(constraints, node)
      a = node.attributes
      
      @relation = ForeignKey.new(
        a['schema'], a['table'], a['name'], 
        on_delete: a['on-delete'], 
        on_update: a['on-update'], 
        enabled: !a['disabled']
      ).tap do |k|
        constraints << k
      end
    end
    
    def handle_referent_element(parse)
      a = parse.node.attributes
      
      @relation.references = [a['schema'], a['name']]
    end
    
    def handle_link_element(parse)
      a = parse.node.attributes
      
      @relation.links << [a['from'], a['to']]
    end
  end
end
