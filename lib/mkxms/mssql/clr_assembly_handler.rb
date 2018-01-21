require 'mkxms/mssql/utils'

module Mkxms; end

module Mkxms::Mssql
  class ClrAssembly
    RaiserrorSource = Utils::RaiserrorWriter.new("%s: Missing or misconfigured assembly %s")
    
    def initialize(name, lib_name = "", access:, owner: nil)
      @name = name
      @error_stmt = RaiserrorSource.next_statement("ERROR".sql_quoted, name.sql_quoted, severity: :error)
      @warning_stmt = RaiserrorSource.next_statement("WARNING".sql_quoted, name.sql_quoted, severity: :warning)
      @lib_name = lib_name
      @access = access
      @owner = owner
    end
    
    attr_reader :name, :error_stmt, :warning_stmt
    attr_accessor :lib_name, :owner, :access
    
    def self.setup_sql
      [].tap do |s|
        s << "IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.object_id = OBJECT_ID(N'xmigra.ignored_clr_assemblies'))"
        s << "    CREATE TABLE xmigra.ignored_clr_assemblies (name SYSNAME PRIMARY KEY);"
        
        s << "" # Give a newline at the end
      end.join("\n")
    end
    
    def to_sql
      [].tap do |s|
        s << "IF NOT EXISTS ("
        s << "  SELECT asm.name"
        s << "  FROM sys.assemblies asm"
        s << "  WHERE asm.is_visible = 1"
        s << "  AND QUOTENAME(asm.name) = #{name.sql_quoted}"
        s << "  UNION ALL"
        s << "  SELECT asm.name"
        s << "  FROM xmigra.ignored_clr_assemblies asm"
        s << "  WHERE asm.name = #{name.sql_quoted}"
        s << ") #{error_stmt};"
        
        s << "IF NOT EXISTS ("
        s << "  SELECT asm.name, QUOTENAME(owner.name) as owner, REPLACE(LOWER(asm.permission_set_desc), '_', '-') as permission_set, asm.clr_name as library"
        s << "  FROM sys.assemblies asm"
        s << "  JOIN sys.database_principals owner ON asm.principal_id = owner.principal_id" if owner
        s << "  WHERE asm.is_visible = 1"
        s << "  AND QUOTENAME(asm.name) = #{name.sql_quoted}"
        s << "  -- #{warning_stmt.error_marker} Run the query up to this point for assembly configuration --"
        cols = [
          ["owner", owner],
          ["permission_set", access],
          ["library", lib_name],
        ].map {|t, v| [t.ljust(v.length), v.ljust(t.length)]}
        s << ("  --                  " + cols.map {|e| e[0]}.join('   ') + ' --')
        s << ("  -- Expected values: " + cols.map {|e| e[1]}.join('   ') + ' --')
        s << "  AND QUOTENAME(owner.name) = #{owner.sql_quoted}" if owner
        s << "  AND REPLACE(LOWER(asm.permission_set_desc), '_', '-') = #{access.sql_quoted}"
        s << "  AND asm.clr_name = #{lib_name.sql_quoted}"
        s << "  UNION ALL"
        s << "  SELECT asm.name, NULL, NULL, NULL"
        s << "  FROM xmigra.ignored_clr_assemblies asm"
        s << "  WHERE asm.name = #{name.sql_quoted}"
        s << ") #{warning_stmt};"
        
        s << "" # Gives a newline at the end
      end.join("\n")
    end
  end
  
  class ClrAssemblyHandler
    def initialize(assemblies, node)
      a = node.attributes
      
      @assembly = ClrAssembly.new(
        a['name'],
        owner: a['owner'],
        access: a['permission-set']
      ).tap do |asm|
        assemblies << asm
      end
    end
    
    def handle_text(content, parent_node)
      @assembly.lib_name << content
    end
  end
end
