require 'mkxms/mssql/index_column'
require 'mkxms/mssql/property_handler'
require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class KeylikeConstraint
    extend Utils::FlagsQueries
    include ExtendedProperties, Property::Hosting
    
    def initialize(attrs)
      @schema = attrs['schema']
      @table = attrs['table']
      @name = attrs['name']
      @stored_on = attrs['stored-on']
      @fill_factor = attrs['fill-factor']
      
      @flags = []
      @flags << :clustered if attrs['clustered']
      @flags << :paddedd if attrs['padded']
      @flags << :row_locks_ok unless attrs['no-row-locks']
      @flags << :page_locks_ok unless attrs['no-page-locks']
      
      @columns = []
    end
    
    attr_accessor :schema, :table, :name, :stored_on, :fill_factor
    attr_reader :columns, :flags
    flags_query :clustered, :padded, :row_locks_ok, :page_locks_ok
    
    def to_sql
      "ALTER TABLE #@schema.#@table ADD #{"CONSTRAINT #@name " if @name}" +
      "#{self.sql_constraint_type} #{'NON' unless clustered?}CLUSTERED (\n" +
      '    ' + columns.map {|c| c.to_sql}.join(", ") +
      "\n)" +
      with_clause_sql +
      # TODO: Handle partitioned constraints
      "#{" ON #@stored_on" if @stored_on}" +
      ";" +
      (name ? extended_properties_sql.joined_on_new_lines : '')
    end
    
    def with_clause_sql
      options = []
      options << 'PAD_INDEX = ON' if padded?
      options << "FILLFACTOR = #@fill_factor" if fill_factor
      options << 'ALLOW_ROW_LOCKS = OFF' unless row_locks_ok?
      options << 'ALLOW_PAGE_LOCKS = OFF' unless page_locks_ok?
      
      return '' if options.empty?
      return " WITH (\n#{options.join ", "}\n)"
    end
    
    def qualified_table
      "#@schema.#@table"
    end
    
    def qualified_name
      "#@schema.#@name" if @name
    end
    
    def property_subject_identifiers
      @prop_subj_id ||= ['SCHEMA', schema, 'TABLE', table, 'CONSTRAINT', name].map {|s| Utils::unquoted_name(s)}
    end
  end
end
