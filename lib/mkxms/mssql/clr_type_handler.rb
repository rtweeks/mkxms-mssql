require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class ClrType
    include Utils::SchemaQualifiedName
    
    RaiserrorSource = Utils::RaiserrorWriter.new("%s: Missing or misconfigured CLR type %s")
    
    def initialize(schema, name, assembly, clr_class)
      @schema = schema
      @name = name
      @assembly = assembly
      @clr_class = clr_class
      @warning_stmt = RaiserrorSource.next_statement("WARNING".sql_quoted, qualified_name.sql_quoted, severity: :warning)
    end
    
    attr_reader :schema, :name, :warning_stmt
    attr_accessor :assembly, :clr_class
    
    def self.setup_sql
      [].tap do |s|
        s << "IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.object_id = OBJECT_ID(N'xmigra.ignored_clr_types'))"
        s << "    CREATE TABLE xmigra.ignored_clr_types ([schema] SYSNAME, name SYSNAME, CONSTRAINT PK_ignored_clr_types PRIMARY KEY ([schema], name));"
        
        s << "" # Give a newline at the end
      end.join("\n")
    end
    
    def to_sql
      [].tap do |s|
        s << "IF NOT EXISTS ("
        s << "  SELECT t.assembly_qualified_name"
        s << "  FROM sys.assembly_types t"
        s << "  JOIN sys.schemas s ON t.schema_id = s.schema_id"
        s << "  WHERE QUOTENAME(s.name) = #{schema.sql_quoted}"
        s << "  AND QUOTENAME(t.name) = #{name.sql_quoted}"
        s << "  UNION ALL"
        s << "  SELECT N''"
        s << "  FROM xmigra.ignored_clr_types t"
        s << "  WHERE t.[schema] = #{schema.sql_quoted}"
        s << "  AND t.name = #{name.sql_quoted}"
        s << ") CREATE TYPE #{schema}.#{name} EXTERNAL NAME #{assembly}.#{clr_class};"
        
        s << "IF NOT EXISTS ("
        s << "  SELECT CONCAT(s.name, N'.', t.name) as clr_type, QUOTENAME(asm.name) as assembly, QUOTENAME(t.assembly_class) as clr_class"
        s << "  FROM sys.assembly_types t"
        s << "  JOIN sys.schemas s ON t.schema_id = s.schema_id"
        s << "  JOIN sys.assemblies asm ON t.assembly_id = asm.assembly_id"
        s << "  WHERE QUOTENAME(s.name) = #{schema.sql_quoted}"
        s << "  AND QUOTENAME(t.name) = #{name.sql_quoted}"
        s << "  -- #{warning_stmt.error_marker} Run the query up to this point for CLR type configuration --"
        cols = [
          ["assembly", assembly],
          ["clr_class", clr_class],
        ].map {|t, v| [t.ljust(v.length), v.ljust(t.length)]}
        s << ("  --                  " + cols.map {|e| e[0]}.join('   ') + ' --')
        s << ("  -- Expected values: " + cols.map {|e| e[1]}.join('   ') + ' --')
        s << "  AND QUOTENAME(asm.name) = #{assembly.sql_quoted}"
        s << "  AND QUOTENAME(t.assembly_class) = #{clr_class.sql_quoted}"
        s << "  UNION ALL"
        s << "  SELECT CONCAT(t.[schema], N'.', t.name), NULL, NULL"
        s << "  FROM xmigra.ignored_clr_types t"
        s << "  WHERE t.[schema] = #{schema.sql_quoted}"
        s << "  AND t.name = #{name.sql_quoted}"
        s << ") #{warning_stmt};"
        
        s << "" # Give a newline at the end
      end
    end
  end
  
  class ClrTypeHandler
    def initialize(types, node)
      a = node.attributes
      
      @type_info = ClrType.new(
        a['schema'],
        a['name'],
        a['assembly'],
        a['class']
      ).tap {|t| types << t}
    end
  end
end
