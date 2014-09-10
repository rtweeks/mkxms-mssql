require 'pathname'
require 'set'
require 'xmigra'
require 'yaml'

%w[

  check_constraint_handler
  default_constraint_handler
  filegroup_handler
  function_handler
  index_handler
  permission_handler
  primary_key_handler
  property_handler
  role_handler
  schema_handler
  statistics_handler
  stored_procedure_handler
  table_handler
  unique_constraint_handler
  utils
  view_handler
  
].each {|f| require "mkxms/mssql/" + f}

module Mkxms; end

module Mkxms::Mssql
  class Mkxms::Mssql::DatabaseHandler
    extend Utils::InitializedAttributes
    include ExtendedProperties, PropertyHandler::ElementHandler
    
    def initialize(**kwargs)
      @schema_dir = kwargs[:schema_dir] || Pathname.pwd
    end
    
    attr_init(:filegroups, :schemas, :roles, :tables, :column_defaults, :pku_constraints, :check_constraints){[]}
    attr_init(:indexes, :statistics){[]}
    attr_init(:views, :udfs, :procedures){[]}
    attr_init(:permissions){[]}
    
    def handle_database_element(parse)
    end
    
    def handle_filegroup_element(parse)
      parse.delegate_to FilegroupHandler, filegroups
    end
    
    def handle_fulltext_document_type_element(parse)
      # TODO: Check that these types are registered in the target instance
    end
    
    def handle_schema_element(parse)
      parse.delegate_to SchemaHandler, schemas
    end
    
    def handle_role_element(parse)
      parse.delegate_to RoleHandler, roles
    end
    
    def handle_table_element(parse)
      parse.delegate_to TableHandler, tables
    end
    
    def handle_default_constraint_element(parse)
      parse.delegate_to DefaultConstraintHandler, column_defaults
    end
    
    def handle_primary_key_element(parse)
      parse.delegate_to PrimaryKeyHandler, pku_constraints
    end
    
    def handle_unique_constraint_element(parse)
      parse.delegate_to UniqueConstraintHandler, pku_constraints
    end
    
    def handle_check_constraint_element(parse)
      parse.delegate_to CheckConstraintHandler, check_constraints
    end
    
    def handle_index_element(parse)
      parse.delegate_to IndexHandler, indexes
    end
    
    def handle_statistics_element(parse)
      parse.delegate_to StatisticsHandler, statistics
    end
    
    def handle_view_element(parse)
      parse.delegate_to ViewHandler, views
    end
    
    def handle_stored_procedure_element(parse)
      parse.delegate_to StoredProcedureHandler, procedures
    end
    
    def handle_user_defined_function_element(parse)
      parse.delegate_to FunctionHandler, udfs
    end
    
    def handle_granted_element(parse)
      parse.delegate_to PermissionHandler, permissions
    end
    
    def handle_denied_element(parse)
      parse.delegate_to PermissionHandler, permissions
    end
    
    def create_source_files
      dbinfo_path = @schema_dir.join(XMigra::SchemaManipulator::DBINFO_FILE)
      
      if dbinfo_path.exist?
        raise ProgramArgumentError.new("#{@schema_dir} already contains an XMigra schema")
      end
      
      # TODO: Sort dependencies of triggers, views, user defined functions, and
      # stored procedures to determine which ones must be incorporated into a
      # migration (all the ones depended on by any triggers).
      
      # Create schema_dir if it does not exist
      @schema_dir.mkpath
      
      # Create and populate @schema_dir + XMigra::SchemaManipulator::DBINFO_FILE
      dbinfo_path.open('w') do |dbi|
        dbi.puts "system: #{XMigra::MSSQLSpecifics::SYSTEM_NAME}"
      end
      
      # TODO: Create migration to check required filegroups and files
      
      # Migration: Create roles
      create_migration(
        "create-roles",
        "Create roles for accessing the database.",
        (roles.map(&:definition_sql) + roles.map(&:membership_sql)).join("\n"),
        roles.map(&:name).sort
      )
      
      # Migration: Create schemas
      create_migration(
        "create-schemas",
        "Create schemas for containing database objects and controlling access.",
        joined_modobj_sql(schemas),
        schemas.map(&:name).sort
      )
      
      tables.each do |table|
        # Migration: Create table
        qual_name = [table.schema, table.name].join('.')
        create_migration(
          "create-table #{qual_name}",
          "Create #{qual_name} table.",
          table.to_sql,
          [table.schema, qual_name]
        )
      end
      
      # Migration: Add column defaults
      create_migration(
        "add-column-defaults",
        "Add default constraints to table columns.",
        joined_modobj_sql(column_defaults),
        column_defaults.map {|d| [d.schema, d.qualified_table, d.qualified_column, d.qualified_name].compact}.flatten.uniq.sort
      )
      
      # Migration: Add primary key and unique constraints
      create_migration(
        "add-primary-key-and-unique-constraints",
        "Add primary key and unique constraints.",
        joined_modobj_sql(pku_constraints),
        pku_constraints.map {|c| [c.schema, c.qualified_table, c.qualified_name].compact}.flatten.uniq.sort
      )
      
      # Migration: Add check constraints
      create_migration(
        "add-check-constraints",
        "Add check constraints.",
        joined_modobj_sql(check_constraints),
        check_constraints.map {|c| [c.schema, c.qualified_table, c.qualified_name].compact}.flatten.uniq.sort
      )
      
      # Check that no super-permissions reference a view, user-defined function, or stored procedure
      access_object_names = (views + udfs + procedures).map {|ao| ao.qualified_name}
      permissions.map {|p| p.super_permissions}.flatten.select do |p|
        access_object_names.include?(p.target)
      end.group_by {|p| p.target}.tap do |problems|
        raise UnsupportedFeatureError.new(
          "#{problems[0].target} cannot be granted the required permission(s)."
        ) if problems.length == 1
        
        raise UnsupportedFeatureError.new(
          (
            ["The required permissions cannot be granted on:"] +
            problems.map {|p| '    ' + p.target}
          ).join("\n")
        ) unless problems.empty?
      end
      
      # Write a migration with all super-permissions
      super_permissions = permissions.map {|p| p.super_permissions_sql}.inject([], :concat)
      create_migration(
        "add-super-permissions",
        "Add permissions that confound the normal GRANT model.",
        super_permissions.join("\n"),
        permissions.map {|p| p.super_permissions.map(&:target)}.flatten.uniq.sort
      ) unless super_permissions.empty?
      
      indexes.each do |index|
        write_index_def(index)
      end
      
      write_statistics
      
      views.each do |view|
        write_access_def(view, 'view')
      end
      
      udfs.each do |udf|
        write_access_def(udf, 'function')
      end
      
      procedures.each do |procedure|
        write_access_def(procedure, 'stored procedure')
      end
      
      @schema_dir.join(XMigra::SchemaManipulator::PERMISSIONS_FILE).open('w') do |p_file|
        YAML.dump(
          permissions.map do |p|
            p.regular_permissions_graph.map do |k, v|
              [k, {p.subject => v}]
            end.to_h
          end.inject({}) do |r, n|
            r.update(n) {|k, lv, rv| lv.merge rv}
          end,
          p_file
        )
      end
    end
    
    def migration_chain
      @migration_chain ||= XMigra::NewMigrationAdder.new(@schema_dir)
    end
    
    def create_migration(summary, description, sql, change_targets)
      migration_chain.add_migration(
        summary,
        description: description,
        sql: sql,
        changes: change_targets
      )
    end
    
    def joined_modobj_sql(ary, sep: "\n")
      ary.map(&:to_sql).join(sep)
    end
    
    def write_access_def(access_obj, obj_type)
      # Use Psych mid-level emitting API to specify literal syntax for SQL
      def_tree = Psych::Nodes::Mapping.new
      ["define", obj_type, "sql"].each do |s|
        def_tree.children << Psych::Nodes::Scalar.new(s)
      end
      def_tree.children << Psych::Nodes::Scalar.new(access_obj.to_sql, nil, nil, false, true,
                                                    Psych::Nodes::Scalar::LITERAL)
      unless (references = access_obj.respond_to?(:references) ? access_obj.references : []).empty?
        def_tree.children << Psych::Nodes::Scalar.new('referencing')
        def_tree.children << (ref_seq = Psych::Nodes::Sequence.new)
        references.each do |r|
          ref_seq.children << Psych::Nodes::Scalar.new(r)
        end
      end
      
      def_doc = Psych::Nodes::Document.new
      def_doc.children << def_tree
      def_stream = Psych::Nodes::Stream.new
      def_stream.children << def_doc
      
      access_dir = @schema_dir.join(XMigra::SchemaManipulator::ACCESS_SUBDIR)
      access_dir.mkpath
      access_dir.join(access_obj.qualified_name + '.yaml').open('w') do |ao_file|
        def_str = def_stream.to_yaml(nil, line_width: -1)
        require 'pry'; binding.pry unless $NO_MORE_PRYING
        ao_file.puts(def_str)
        #def_stream.to_yaml(ao_file, line_width: -1)
      end
    end
    
    def write_index_def(index)
      indexes_dir = @schema_dir.join(XMigra::SchemaManipulator::INDEXES_SUBDIR)
      indexes_dir.mkpath
      index_path = indexes_dir.join(index.name + '.yaml')
      
      raise UnsupportedFeatureError.new(
        "Index file #{index_path} already exists."
      ) if index_path.exist?
      
      index_path.open('w') do |index_file|
        YAML.dump({'sql' => index.to_sql}, index_file, line_width: -1)
      end
    end
    
    def write_statistics
      statistics_path = @schema_dir.join(XMigra::MSSQLSpecifics::STATISTICS_FILE)
      
      statistics_path.open('w') do |stats_file|
        YAML.dump(
          Hash[statistics.map(&:name_params_pair)],
          stats_file,
          line_width: -1
        )
      end
    end
  end
end
