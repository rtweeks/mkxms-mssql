require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class Index
    extend Utils::FlagsQueries
    include ExtendedProperties, Property::Hosting
    
    def initialize(attrs)
      @schema = attrs['schema']
      @relation = attrs['relation']
      @name = attrs['name']
      @fill_factor = attrs['fill-factor']
      @spatial_index_geometry = attrs['spatial-index-over']
      @cells_per_object = attrs['cells-per-object']
      @storage = attrs['stored-on']
      @columns = []
      @included_columns = []
      
      @flags = []
      @flags << :unique if attrs['unique']
      @flags << :padded if attrs['padded']
      @flags << :disabled if attrs['disabled']
      @flags << :ignore_duplicates if attrs['ignore-duplicates']
      @flags << :row_locks_prohibited if attrs['no-row-locks']
      @flags << :page_locks_prohibited if attrs['no-page-locks']
    end
    
    attr_accessor :schema, :relation, :name, :fill_factor, :spatial_index_geometry, :cells_per_object, :storage
    attr_reader :columns, :included_columns, :flags
    
    flags_query :unique, :padded, :ignore_duplicates, :row_locks_prohibited, :page_locks_prohibited
    
    def to_sql
      if @spatial_index_geometry
      else
        [].tap do |parts|
          parts << "CREATE #{'UNIQUE ' if unique?}INDEX #@name ON #{qualified_relation} (\n" +
          @columns.map(&:to_sql).join(', ') +
          "\n)"
          
          parts << "INCLUDE (\n" +
          @included_columns.map(&:name).join(', ') +
          "\n)" unless @included_columns.empty?
          
          # TODO: "WHERE" clause
          
          options = []
          options << "PAD_INDEX = ON" if padded?
          options << "FILLFACTOR = #@fill_factor" if @fill_factor
          options << "IGNORE_DUP_KEY = ON" if ignore_duplicates?
          options << "ALLOW_ROW_LOCKS = OFF" if row_locks_prohibited?
          options << "ALLOW_PAGE_LOCKS = OFF" if page_locks_prohibited?
          parts << "WITH (#{options.join(', ')})" unless options.empty?
          
          parts << "ON #@storage" if @storage
          
        end.join(' ') + ';' + extended_properties_sql.joined_on_new_lines
      end
    end
    
    def property_subject_identifiers
      ['SCHEMA', @schema, 'TABLE', @relation, 'INDEX', @name].map {|n| Utils.unquoted_name(n)}
    end
    
    def qualified_relation
      [@schema, @relation].join '.'
    end
  end
  
  class IndexHandler
    include PropertyHandler::ElementHandler
    
    def initialize(indexes, node)
      @index = Index.new(node.attributes).tap do |i|
        store_properties_on i
        indexes << i
      end
    end
    
    def handle_column_element(parse)
      a = parse.node.attributes
      
      if a['included']
        @index.included_columns << IndexColumn.new(a['name'])
      else
        @index.columns << IndexColumn.new(a['name'], a['desc'] ? :descending : :ascending)
      end
    end
    
    # TODO: Handle partition columns
  end
end
