require 'pathname'
require 'psych' # YAML support
require 'xmigra'

module Mkxms; end

module Mkxms::Mssql
  class DeclarativesCreator
    def initialize(document, schema_dir)
      @document = document
      @schema_dir = schema_dir || Pathname.pwd
    end
    
    def decls_dir
      @schema_dir.join(
        XMigra::SchemaManipulator::STRUCTURE_SUBDIR,
        XMigra::DeclarativeMigration::SUBDIR,
      )
    end
    
    def create_artifacts
      index_constraints
      
      # Loop through all tables
      decl_paths = []
      @document.elements.each('/database/table') do |table|
        schema, name = %w[schema name].map {|a| table.attributes[a]}
        tdecl_path = decls_dir.join([schema, name, 'yaml'].join('.'))
        doc = build_declarative(table)
        decls_dir.mkpath
        tdecl_path.open('w') {|f| f.write(doc.to_yaml)}
        decl_paths << tdecl_path
      end
      
      # Loop through the created paths creating an adoption migration for each
      decl_paths.each do |fpath|
        tool = XMigra::ImpdeclMigrationAdder.new(@schema_dir)
        tool.add_migration_implementing_changes(fpath, {adopt: true})
      end
    end
    
    def build_declarative(table)
      doc, tdecl = create_blank_table_decl
      
      columns_decl = Psych::Nodes::Sequence.new.tap do |s|
        tdecl.children << node_from('columns') << s
      end
      
      table_key = %w[schema name].map {|a| table.attributes[a]}
      
      # Columns (including single-column default constraints)
      table.elements.each('column') do |column|
        entry = Psych::Nodes::Mapping.new.tap {|e| columns_decl.children << e}
        entry.children.concat(
          ['name', column.attributes['name']].map {|v| node_from(v)}
        )
        col_type = column.attributes['type']
        if capacity = column.attributes['capacity']
          col_type = "#{col_type}(#{capacity})"
        end
        entry.children.concat(
          ['type', col_type].map {|v| node_from(v)}
        )
        unless column.attributes['nullable']
          entry.children.concat(
            ['nullable', false].map {|v| node_from(v)}
          )
        end
        
        if cstr = cstr_on_column(@default_constraints, table_key, column)
          entry.children.concat(['default', cstr.text].map {|v| node_from(v)})
        end
        if cexpr = column.elements['computed-expression']
          entry.children.concat(['X-computed-as', cexpr.text].map {|v| node_from(v)})
        end
      end
      
      # Everything but default constraints
      cstrs_decl = Psych::Nodes::Mapping.new
      constraint_default_name_part = mashable_name(table.attributes['name'])
      @primary_key_constraints.fetch(table_key, []).each do |cstr|
        cstr_name = cstr.attributes['name'] || "PK_#{constraint_default_name_part}"
        cstrs_decl.children << node_from(cstr_name) << node_from({
          'type' => 'primary key',
          'columns' => cstr.elements.enum_for(:each, 'column').map {|c| c.attributes['name']},
        })
      end
      @uniqueness_constraints.fetch(table_key, []).each do |cstr|
        cstr_name = cstr.attributes['name'] || (
          "UQ_#{constraint_default_name_part}_" +
          mashable_name(
            cstr.elements.enum_for(:each, 'column').map {|c| c.attributes['name']}.join('_')
          )
        )
        cstrs_decl.children << node_from(cstr_name) << node_from({
          'type' => 'unique',
          'columns' => cstr.elements.enum_for(:each, 'column').map {|c| c.attributes['name']},
        })
      end
      @foreign_key_constraints.fetch(table_key, []).each do |cstr|
        cstr_name = cstr.attributes['name'] || :generated
        if cstr_name == :generated
          from_cols, to_cols = [], []
          cstr.elements.each('link') do |link|
            from_cols << link.attributes['from']
            to_cols << link.attributes['to']
          end
          cstr_name = (
            "FK_#{constraint_default_name_part}_" + 
            mashable_name(from_cols.join('_')) + '_' +
            mashable_name(%w[schema name].map {|a| cstr.elements['referent'].attributes[a]}.join('_')) + '_' +
            mashable_name(to_cols.join('_'))
          )
        end
        cstrs_decl.children << node_from(cstr_name) << node_from({
          'link to' => cstr.elements['referent'].tap do |r|
            break [r.attributes['schema'], r.attributes['name']].join('.')
          end,
          'columns' => Hash[
            cstr.elements.enum_for(:each, 'link').map do |link|
              %w[from to].map {|a| link.attributes[a]}
            end
          ],
        })
      end
      existing_check_names = nil
      @check_constraints.fetch(table_key, []).each_with_index do |cstr, i|
        cstr_name = cstr.attributes['name'] || :generated
        if cstr_name == :generated
          existing_check_names ||= @check_constraints[table_key].map {|c| c.attributes['name']}.compact
          cstr_name = "CK_#{constraint_default_name_part}_#{i+1}"
          while existing_check_names.include?(cstr_name)
            cstr_name << '_' unless cstr_name.end_with?('_')
            cstr_name << 'X'
          end
          existing_check_names << cstr_name
        end
        cstrs_decl.children << node_from(cstr_name) << node_from({
          'verify' => cstr.text,
        })
      end
      
      unless cstrs_decl.children.empty?
        tdecl.children << node_from("constraints") << cstrs_decl
      end
      
      return doc
    end
    
    def index_constraints
      @primary_key_constraints = read_constraints('primary-key')
      @uniqueness_constraints = read_constraints('unique-constraint')
      @foreign_key_constraints = read_constraints('foreign-key')
      @check_constraints = read_constraints('check-constraint')
      @default_constraints = read_constraints('default-constraint')
    end
    
    def read_constraints(ctype, inline: false)
      @document.elements.enum_for(:each, "/database/#{ctype}").each_with_object({}) do |cstr, result|
        key = [cstr.attributes['schema'], cstr.attributes['table']]
        (result[key] ||= []) << cstr
      end
    end
    
    def create_blank_table_decl
      stream = Psych::Nodes::Stream.new
      doc = Psych::Nodes::Document.new.tap {|d| stream.children << d}
      decl = Psych::Nodes::Mapping.new.tap {|m| doc.children << m}
      decl.implicit = false
      decl.tag = '!table'
      return [stream, decl]
    end
    
    def node_from(val)
      ast_stream = Psych.parse_stream(Psych.dump(val))
      return ast_stream.children[0].children[0]
    end
    
    def attr_eq?(a, o1=nil, *objs)
      return true if o1.nil? || objs.length == 0
      val = o1.attributes[a]
      return objs.all? {|o| o.attributes[a] == val}
    end
    
    def cstr_on_column(group, key, column)
      cstrs = group[key]
      return nil unless cstrs
      cstrs.find do |cstr|
        cstr.attributes['column'] == column.attributes['name'] || \
          cstr.elements.enum_for(:each, 'column').select {|c| attr_eq?('name', c, column)}.count > 0
      end
    end
    
    def mashable_name(s)
      s.gsub(/[\]\[]/, '').gsub(/[^a-zA-Z_]/, '_')
    end
  end
end
