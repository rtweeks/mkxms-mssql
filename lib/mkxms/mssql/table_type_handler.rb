require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class TableType
    class Column
      include ExtendedProperties
      extend Utils::InitializedAttributes
      
      SQL_OBJECT_TYPE = 'COLUMN'
      
      def initialize(attrs)
        a = attrs
        @name = a['name']
        @type_schema = a['type-schema']
        @type_name = a['type']
        @capacity = a['capacity']
        @capacity = @capacity.to_i unless @capacity.nil? || @capacity == 'max'
        @precision = a['precision']
        @scale = a['scale']
        @collation = a['collation']
        @nullable = !!a['nullable']
        @ansi_padded = !a['not-ansi-padded']
        @full_xml_document = !!a['full-xml-document']
        @xml_schema_collection = a['xml_collection_id']
      end
      
      attr_accessor :name, :type_schema, :type_name, :capacity, :precision, :scale, :collation, :xml_schema_collection
      attr_accessor :computed_expression
      attr_init(:check_constraints) {[]}
      
      def nullable?; @nullable; end
      def nullable=(val); @nullable = !!val; end
      
      def ansi_padded?; @ansi_padded; end
      def ansi_padded=(val); @ansi_padded = !!val; end
      
      def full_xml_document?; @full_xml_document; end
      def full_xml_document=(val); @full_xml_document = !!val; end
      
      def type_spec
        [type_schema, type_name].compact.join('.').tap do |result|
          result << "(#{capacity})" if capacity
          result << " COLLATE #{collation}" if collation
          result << "(#{[precision, scale].compact.join(', ')})" if precision
          result << ' NOT NULL' unless nullable?
          check_constraints.each do |c|
            result << " #{c.to_sql}"
          end
        end
      end
      
      def max_byte_consumption
        if [nil, '[sys]'].include?(type_schema) && %w[[nchar] [nvarchar]].include?(type_name)
          2 * capacity
        else
          capacity
        end
      end
    end
    
    class ConstraintColumn
      def initialize(attrs)
        @name = attrs['name']
        @ascending = !attrs['desc']
      end
      
      attr_accessor :name
      
      def ascending?; @ascending; end
      def descending?; !@ascending; end
      def ascending=(val); @ascending = !!val; end
      def descending=(val); @ascending = !val; end
      
      def spec
        "#{name} #{ascending? ? "ASC" : "DESC"}"
      end
    end
    
    class KeyConstraint
      include ExtendedProperties
      extend Utils::InitializedAttributes
      
      SQL_OBJECT_TYPE = 'CONSTRAINT'
      
      def initialize(attrs)
        @type = attrs['type']
        @clustered = !!attrs['clustered']
        @ignore_duplicates = !!attrs['ignore-duplicates']
      end
      
      attr_accessor :type, :ignore_duplicates
      attr_init(:columns) {[]}
      
      def clustered?; @clustered; end
      def clustered=(val); @clustered = !!val; end
      
      def ignore_duplicates?; @ignore_duplicates; end
      def ignore_duplicates=(val); @ignore_duplicates = !!val; end
      
      def to_sql
        "#{type} #{clustered? ? "CLUSTERED" : "NONCLUSTERED"} (#{
          columns.map(&:spec).join(', ')
        })".tap do |result|
          result << " WITH (IGNORE_DUP_KEY = ON)" if ignore_duplicates?
        end
      end
    end
    
    class CheckConstraint
      include ExtendedProperties
      extend Utils::InitializedAttributes
      
      SQL_OBJECT_TYPE = 'CONSTRAINT'
      
      def initialize(attrs)
      end
      
      attr_init(:expression) {''}
      
      def type
        "CHECK"
      end
      
      def to_sql
        "CHECK #{expression}"
      end
    end
    
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    include Utils::SchemaQualifiedName
    extend Utils::InitializedAttributes
    
    SQL_OBJECT_TYPE = 'TYPE'
    
    def initialize(attrs)
      a = attrs
      info_ver = (a['eyewkas_ver'] || 1.0).to_f
      raise "mssql-eyewkas table-type ver. 1.1 or compatible required" if info_ver < 1.1 || info_ver >= 2
      @schema = a['schema']
      @name = a['name']
    end
    
    attr_accessor :schema, :name
    attr_init(:columns, :constraints) {[]}
    
    def to_sql
      [].tap do |lines|
        lines << "CREATE TYPE #{qualified_name} AS TABLE ("
        columns.each_with_index do |col, i|
          lines << "  #{i == 0 ? " " : ","} #{col.name} #{col.type_spec}"
        end
        constraints.each do |c|
          lines << "  , #{c.to_sql}"
        end
        lines << ");"
        lines << extended_properties_sql
        columns.each do |col|
          lines << subitem_extended_properties_sql(col)
        end
      end
    end
  end
  
  class TableTypeColumnHandler
    include PropertyHandler::ElementHandler
    
    def initialize(column)
      store_properties_on(@column = column)
    end
    
    def handle_computed_expression_element(parse)
      # Do nothing
    end
    
    def handle_check_constraint_element(parse)
      parse.delegate_to TableTypeCheckConstraintHandler, @column.check_constraints
    end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'computed-expression']
        (@column.computed_expression ||= '') << text
      end
    end
  end
  
  class TableTypeCheckConstraintHandler
    def initialize(constraints, node)
      TableType::CheckConstraint.new(node.attributes).tap do |c|
        constraints << (@constraint = c)
      end
    end
    
    def handle_expression_element(parse)
      # do nothing
    end
    
    def handle_text(text, parent_element)
      case [parent_element.namespace, parent_element.name]
      when ['', 'expression']
        @constraint.expression << text
      end
    end
    
    def handle_property_element(parse)
      raise "Properties on table type constraints are unsupported"
    end
  end
  
  class TableTypeKeyConstraintHandler
    def initialize(constraints, node)
      TableType::KeyConstraint.new(node.attributes).tap do |c|
        constraints << (@constraint = c)
      end
    end
    
    def handle_column_element(parse)
      @constraint.columns << TableType::ConstraintColumn.new(parse.node.attributes)
    end
    
    def handle_property_element(parse)
      raise "Properties on table type constraints are unsupported"
    end
  end
  
  class TableTypeHandler
    include PropertyHandler::ElementHandler
    
    def initialize(user_types, node)
      TableType.new(node.attributes).tap do |tt|
        user_types << store_properties_on(@type = tt)
      end
    end
    
    def handle_column_element(parse)
      a = parse.node.attributes
      TableType::Column.new(parse.node.attributes).tap do |c|
        @type.columns << c
        parse.context = TableTypeColumnHandler.new(c)
      end
    end
    
    def handle_key_constraint_element(parse)
      parse.delegate_to TableTypeKeyConstraintHandler, @type.constraints
    end
    
    def handle_check_constraint_element(parse)
      parse.delegate_to TableTypeCheckConstraintHandler, @type.constraints
    end
  end
end
