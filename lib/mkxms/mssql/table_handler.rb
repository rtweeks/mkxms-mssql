require 'mkxms/mssql/exceptions'
require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Table
    SQL_OBJECT_TYPE = 'TABLE'
    include ExtendedProperties, Property::Hosting, Property::SchemaScoped
    
    def initialize(schema, name)
      @schema = schema
      @name = name
      @columns = []
    end
    
    attr_accessor :schema, :name, :owner, :heap_storage, :lob_storage
    attr_reader :columns
    
    def to_sql
      lines = ["CREATE TABLE #{schema}.#{name} ("]
      lines << columns.map{|c| "    " + c.to_sql}.join(",\n")
      lines << ")"
      
      lines << "ON #{heap_storage}" if heap_storage
      lines << "TEXTIMAGE_ON #{lob_storage}" if lob_storage
      
      lines << ";"
      lines << ""
      
      if owner
        lines << ["ALTER AUTHORIZATION ON OBJECT::#{schema}.#{name} TO #{owner};"]
        lines << ""
      end
      
      lines.concat extended_properties_sql
      
      columns.each do |col|
        lines.concat subitem_extended_properties_sql(col)
      end
      
      return lines.join("\n")
    end
  end

  class Column
    SQL_OBJECT_TYPE = 'COLUMN'
    
    include ExtendedProperties
    extend Utils::FlagsQueries
    
    def initialize(name)
      @name = name
      @flags = []
      @type_info = {}
    end
    
    attr_accessor :name, :type, :collation, :computed_expression
    attr_reader :flags, :type_info
    
    flags_query :filestream, :nullable, :identity, :replicated, :rowguid, :persisted
    
    def to_sql
      parts = [name]
      if computed_expression
        parts << "AS " + computed_expression
        parts << "PERSISTED" if persisted?
      else
        each_type_part {|part| parts << part}
      end
      
      return parts.join(' ')
    end
    
    def each_type_part
      yield type
      yield("COLLATE " + collation) if collation
      yield(nullable? ? 'NULL' : 'NOT NULL')
      if identity?
        yield "IDENTITY"
        yield("NOT FOR REPLICATION") unless replicated?
      end
      yield("ROWGUID") if rowguid?
    end
  end

  class TableHandler
    class ColumnHandler
      include PropertyHandler::ElementHandler
      
      def initialize(columns, node)
        a = node.attributes
        
        col_attrs = {}
        use_attr = proc {|k| col_attrs[k.gsub('-', '_').to_sym] = node.attributes[k]}
        col_type = %w[type-schema type].map {|k| use_attr[k]}.compact.join('.')
        
        if a.has_key?('capacity')
          col_type << "(%s)" % [use_attr['capacity']]
        end
        
        prec_scale = []
        if a.has_key?('precision') || a.has_key?('scale')
          prec_scale << use_attr['precision']
        end
        if a.has_key?('scale')
          prec_scale << use_attr['scale']
        end
        unless prec_scale.empty?
          col_type << "(%s)" % (prec_scale.join(', '))
        end
        
        if a.has_key?('xml_collection_id')
          col_type << "(%s %s)" % [
            xml_structure = (a['full-xml-document'] ? 'DOCUMENT' : 'CONTENT'),
            a['xml_collection_id']
          ]
          col_attrs[:xml_validation] = {xml_structure.downcase => a['xml_collection_id']}
        end
        
        raise UnsupportedFeatureError.new("Column #{name} declared 'not-ansi-padded'") if a['not-ansi-padded']
        
        @column = Column.new(a['name']).tap do |c|
          c.type = col_type
          c.collation = a['collation']
          c.flags << :nullable if a['nullable']
          c.flags << :replicated if a['replicated']
          c.flags << :filestream if a['filestream']
          c.type_info.update(col_attrs)
          store_properties_on c
          columns << c
        end
      end
      
      attr_reader :column
      
      def handle_computed_expression_element(parse)
        column.flags << :persisted if parse.node.attributes['persisted']
        # Handle expression in #handle_text
      end
      
      def handle_text(text, parent_element)
        case %i[namespace name].map {|m| parent_element.send(m)}
        when ['', 'computed-expression']
          (column.computed_expression ||= '') << text
        end
      end
    end
    
    include PropertyHandler::ElementHandler
    
    def initialize(tables, node)
      a = node.attributes
      @table = Table.new(a['schema'], a['name']).tap do |t|
        store_properties_on t
        tables << t
      end
      @table.owner = a['owner']
      @table.heap_storage = a['rows-on']
      @table.lob_storage = a['textimage-on']
      @identity_column = a['identity']
      @rowguid_column = a['rowguidcol']
    end
    
    def handle_column_element(parse)
      parse.context = ColumnHandler.new(@table.columns, parse.node)
      column = parse.context.column
      
      column.flags << :identity if column.name.eql? @identity_column
      column.flags << :rowguid if column.name.eql? @rowguid_column
    end
  end
end
