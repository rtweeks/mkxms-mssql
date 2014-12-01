require 'pathname'
require 'xmigra'
require 'mkxms/mssql/indented_string_builder'
require 'mkxms/mssql/query_cursor'
require 'mkxms/mssql/sql_string_manipulators'

module Mkxms; end

module Mkxms::Mssql
  class AdoptionScriptWriter
    include XMigra::MSSQLSpecifics
    include SqlStringManipulators
    
    def initialize(db_expectations)
      @db_expectations = db_expectations
      # Ex nihilo DB schema builder
      @xn_builder = XMigra::SchemaUpdater.new(@db_expectations.schema_dir)
    end
    
    attr_reader :db_expectations
    
    def create_script(path)
      Pathname(path).open('w') do |script|
        script.puts adoption_sql
      end
    end
    
    def adoption_sql
      in_ddl_transaction do
        script_parts = [
          # Check for blatantly incorrect application of script, e.g. running
          # on master or template database.
          :check_execution_environment_sql,
          
          # Create schema version control (SVC) tables if they don't exist
          :ensure_version_tables_sql,
          :ensure_permissions_table_sql,
          
          # Create an error table
          :create_adoption_error_table_sql,
          
          # Check roles
          :check_expected_roles_exist_sql,
          :check_expected_role_membership_sql,
          
          # Check schemas
          :check_expected_schemas_exist_sql,
          
          # Check tables (including columns)
          :check_tables_exist_and_structured_as_expected_sql,
          
          # Check column defaults
          :check_expected_column_defaults_exist_sql,
          
          # Check primary key and unique constraints
          :check_primary_key_and_unique_constraints_sql,
          
          # Check foreign key constraints
          :check_foreign_key_constraints_sql,
          
          # Check check constraints
          :check_check_constraints_sql,
          
          # Adopt indexes
          :adopt_indexes_sql,
          
          # Adopt statistics
          :adopt_statistics_sql,
          
          # Adopt views
          :adopt_views_sql,
          
          # Adopt stored procedures
          :adopt_stored_procedures_sql,
          
          # Adopt user defined functions
          :adopt_udfs_sql,
          
          # Adopt permissions
          :adopt_permissions_sql,
          
          # Error out if there are any entries in the error table
          :check_adoption_error_table_empty_sql,
          
          # Write version bridge record to xmigra.applied
          :write_version_bridge_record_sql,
        ]
        
        #script_parts = script_parts.map {|mn| self.send(mn)}.flatten.compact
        script_parts = script_parts.map do |mn|
          [
            %Q{PRINT N'ADOPTION STEP: #{mn}';},
            self.send(mn)
          ]
        end.flatten.compact
        script_parts.join(ddl_block_separator)
      end
    end
    
    def compose_sql(&blk)
      IndentedStringBuilder.dsl(&blk)
    end
    
    begin # Adoption error handling methods
      def create_adoption_error_table_sql
        dedent %Q{
          IF EXISTS (
            SELECT * FROM sys.objects o WHERE o.object_id = OBJECT_ID(N'[xmigra].[adoption_errors]')
          )
          BEGIN
            DROP TABLE [xmigra].[adoption_errors];
          END;
          GO
          
          CREATE TABLE [xmigra].[adoption_errors] (
            [message]          nvarchar(1000)
          );
        }
      end
      
      def adoption_error_sql(message)
        "INSERT INTO [xmigra].[adoption_errors] (message) VALUES (#{strlit(message)});"
      end
      
      def check_adoption_error_table_empty_sql
        dedent %Q{
          IF EXISTS (
            SELECT TOP 1 * FROM [xmigra].[adoption_errors]
          )
          BEGIN
            SELECT * FROM [xmigra].[adoption_errors];
            RAISERROR (N'Database adoption failed.', 11, 1);
          END;
          
          DROP TABLE [xmigra].[adoption_errors];
        }
      end
    end
    
    def check_expected_roles_exist_sql
      db_expectations.roles.map do |r|
        dedent %Q{
          IF NOT EXISTS (
            SELECT * FROM sys.database_principals r
            WHERE r.name = #{strlit(unquoted_identifier r.name)}
            AND r.type = 'R'
          )
          BEGIN
            #{adoption_error_sql "Role #{r.name} does not exist."}
          END;
          
          IF EXISTS (
            SELECT * FROM sys.database_principals r
            INNER JOIN sys.database_principals o ON r.owning_principal_id = o.principal_id
            WHERE r.name = #{strlit(unquoted_identifier r.name)}
            AND o.name <> #{strlit(unquoted_identifier r.owner)}
          )
          BEGIN
            #{adoption_error_sql "Role #{r.name} should be owned by #{r.owner}."}
          END;
        }
      end.join("\n")
    end
    
    def check_expected_role_membership_sql
      [].tap do |tests|
        db_expectations.roles.each do |r|
          r.encompassing_roles.each do |er_name|
            tests << dedent(%Q{
              IF NOT EXISTS (
                SELECT * FROM sys.database_role_members rm
                INNER JOIN sys.database_principals r ON rm.member_principal_id = r.principal_id
                INNER JOIN sys.database_principals er ON rm.role_principal_id = er.principal_id
                WHERE r.name = #{strlit(unquoted_identifier r.name)} 
                AND er.name = #{strlit(unquoted_identifier er_name)}
              )
              BEGIN
                #{adoption_error_sql "Role #{r.name} should be a member of #{er_name}."}
              END;
            })
          end
        end
      end.join("\n")
    end
    
    def check_expected_schemas_exist_sql
      db_expectations.schemas.map do |schema|
        dedent %Q{
          IF NOT EXISTS (
            SELECT * FROM sys.schemas s
            WHERE s.name = #{strlit(unquoted_identifier schema.name)}
          )
          BEGIN
            #{adoption_error_sql "Schema #{schema.name} does not exist."}
          END ELSE IF NOT EXISTS (
            SELECT * FROM sys.schemas s
            INNER JOIN sys.database_principals r ON s.principal_id = r.principal_id
            WHERE s.name = #{strlit(unquoted_identifier schema.name)}
            AND r.name = #{strlit(unquoted_identifier schema.owner)}
          )
          BEGIN
            #{adoption_error_sql "Schema #{schema.name} is not owned by #{schema.owner}."}
          END;
        }
      end
    end
    
    class TableAdoptionChecks < IndentedStringBuilder
      include SqlStringManipulators
      extend SqlStringManipulators
      
      def initialize(table, error_sql_proc)
        super()
        
        @table = table
        @schema_name_literal = strlit(unquoted_identifier table.schema)
        @table_name_literal = strlit(unquoted_identifier table.name)
        @table_id = [table.schema, table.name].join('.')
        @error_sql_proc = error_sql_proc
        
        add_table_tests
      end
      
      attr_reader :table, :schema_name_literal, :table_name_literal, :table_id
      
      def error_sql(s)
        @error_sql_proc.call(s)
      end
      
      def add_table_tests
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.tables t
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE t.name = #{table_name_literal}
              AND s.name = #{schema_name_literal}
            }
          end
          puts "BEGIN"
          indented {
            puts error_sql "Table #{table_id} does not exist."
          }
          puts "END ELSE IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.tables t
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              LEFT JOIN sys.database_principals r ON t.principal_id = r.principal_id
              WHERE t.name = #{table_name_literal}
              AND s.name = #{schema_name_literal}
              AND r.name #{table.owner ? "= " + strlit(unquoted_identifier(table.owner)) : "IS NULL"}
            }
          end
          puts "BEGIN"
          indented {
            puts error_sql(
              if table.owner
                "Table #{table_id} is not owned (explicitly) by #{table.owner}."
              else
                "Table #{table_id} is specified as other than the schema owner."
              end
            )
          }
          puts "END;"
          puts
        }
        QueryCursor.new(
          dedent(%Q{
            SELECT c.object_id, c.column_id
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = #{table_name_literal}
            AND s.name = #{schema_name_literal}
            ORDER BY c.column_id;
          }),
          "@column_object INT, @column_id INT",
          output_to: self
        ).expectations(
          on_extra: ->{puts error_sql "Table #{table_id} has one or more unexpected columns."},
        ) do |test|
          table.columns.each do |column|
            test.row(
              on_missing: ->{puts error_sql "Column #{column.name} not found where expected in #{table_id}."},
            ) {add_column_tests(column)}
          end
        end
      end
      
      def add_column_tests(column)
        column_name_literal = strlit(unquoted_identifier column.name)
        
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.columns c
              WHERE c.object_id = @column_object
              AND c.column_id = @column_id
              AND c.name = #{column_name_literal}
            }
          end
          puts "BEGIN"
          indented {
            puts dedent %Q{
              SET @column_id = (
                SELECT c.column_id FROM sys.columns c
                WHERE c.object_id = @column_object
                AND c.name = #{column_name_literal}
              );
            }
            puts "IF @column_id IS NULL"
            puts "BEGIN"
            indented {
              puts error_sql "Column #{column.name} not found in #{table_id}."
            }
            puts "END ELSE BEGIN"
            indented {
              puts error_sql "Column #{column.name} not found in expected position in #{table_id}."
            }
            puts "END;"
          }
          puts "END;"
          puts "IF @column_id IS NOT NULL"
          puts "BEGIN".."END;" do
            add_column_properties_test(column)
          end
        }
      end
      
      NON_ANSI_PADDABLE_TYPES = %w[char varchar binary varbinary]
      UNICODE_CHAR_TYPES = %w[nchar nvarchar]
      def add_column_properties_test(column)
        conditions = []
        if column.computed_expression
          mismatch_message = "does not have the expected definition"
          
          conditions << %Q{c.is_computed = 1}
          conditions << compose_sql {
            puts "EXISTS (SELECT * FROM sys.computed_columns cc WHERE %s)" do
              puts "AND cc.object_id = c.object_id"
              puts "AND cc.column_id = c.column_id"
              puts "AND cc.definition = %s" do
                strlit(column.computed_expression)
              end
              puts "AND %s" do
                bit_test "cc.is_persisted", column.persisted?
              end
            end
          }
        else
          type_str = [].tap {|parts| column.each_type_part {|part| parts << part}}.join(' ')
          mismatch_message = "is not #{type_str}"
          
          conditions << "ct.name = %s" % [strlit(unquoted_identifier column.type_info[:type])]
          type_schema = column.type_info[:type_schema] || 'sys'
          col_type_is_sys_type = unquoted_identifier(type_schema).downcase == 'sys'
          comparable_col_type = unquoted_identifier(column.type_info[:type]).downcase
          conditions << compose_sql {
            puts "EXISTS (SELECT * FROM sys.schemas cts WHERE %s)" do
              puts "cts.schema_id = ct.schema_id"
              puts "AND cts.name = #{strlit(unquoted_identifier type_schema)}"
            end
          }
          if precision = column.type_info[:precision]
            conditions << %Q{c.precision = #{precision}}
          end
          if scale = column.type_info[:scale]
            conditions << %Q{c.scale = #{scale}}
          end
          if capacity = column.type_info[:capacity]
            conditions << (if capacity == 'max'
              %Q{c.max_length = -1}
            elsif col_type_is_sys_type && %w[nchar nvarchar].include?(comparable_col_type)
              %Q{c.max_length = #{capacity.to_i * 2}}
            else
              %Q{c.max_length = #{capacity}}
            end)
          end
          conditions << %Q{c.collation_name = #{strlit column.collation}} if column.collation
          conditions << bit_test("c.is_identity", column.identity?)
          conditions << bit_test("c.is_rowguidcol", column.rowguid?)
          conditions << bit_test("c.is_filestream", column.filestream?)
          conditions << bit_test("c.is_nullable", column.nullable?)
          if col_type_is_sys_type && NON_ANSI_PADDABLE_TYPES.include?(comparable_col_type)
            conditions << bit_test("c.is_ansi_padded", true)
          end
        end
        
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.columns c
              INNER JOIN sys.types ct ON c.user_type_id = ct.user_type_id
              WHERE c.object_id = @column_object
              AND c.column_id = @column_id
            }
            conditions.each {|c| puts "AND " + c, :sub => nil}
          end
          puts "BEGIN".."END;" do
            puts error_sql "Column #{column.name} of #{table_id} #{mismatch_message}"
          end
        }
      end
      
      def compose_sql(&blk)
        IndentedStringBuilder.dsl(&blk)
      end
    end
    
    def check_tables_exist_and_structured_as_expected_sql
      db_expectations.tables.map do |table|
        TableAdoptionChecks.new(table, method(:adoption_error_sql)).to_s
      end # Do not join -- each needs a separate batch (they use variables)
    end
    
    def check_expected_column_defaults_exist_sql
      db_expectations.column_defaults.map do |col_dflt|
        constraint_id = (col_dflt.name || "on #{col_dflt.column}") + " of #{col_dflt.qualified_table}"
        compose_sql {
          puts "IF NOT EXISTS (%s)" do
            puts "SELECT * FROM sys.default_constraints dc"
            puts "INNER JOIN sys.schemas s ON dc.schema_id = s.schema_id"
            puts "INNER JOIN sys.tables t ON dc.parent_object_id = t.object_id"
            puts "INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id"
            puts "WHERE dc.name = %s" do
              puts strlit(unquoted_identifier col_dflt.name)
            end if col_dflt.name
          end
          puts "BEGIN"
          indented {puts adoption_error_sql(
            "Expected column default constraint #{constraint_id} does not exist."
          )}
          puts "END ELSE BEGIN"
          indented {
            puts "IF NOT EXISTS (%s)" do
              puts "SELECT * FROM sys.default_constraints dc"
              puts "INNER JOIN sys.schemas s ON dc.schema_id = s.schema_id"
              puts "INNER JOIN sys.tables t ON dc.parent_object_id = t.object_id"
              puts "INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id"
              puts "WHERE dc.definition = %s" do
                puts strlit col_dflt.expression
              end
              puts "AND dc.name = %s" do
                puts strlit(unquoted_identifier col_dflt.name)
              end if col_dflt.name
            end
            puts("BEGIN".."END;") {
              puts adoption_error_sql("Column default constraint #{constraint_id} does not have the expected definition.")
            }
          }
          puts "END;"
        }
      end.join("\n")
    end
    
    class KeylikeConstraintAdoptionChecks < IndentedStringBuilder
      include SqlStringManipulators
      
      def initialize(cnstr, error_sql_proc)
        super()
        
        @cnstr = cnstr
        @error_sql_proc = error_sql_proc
        @constraint_type = cnstr.sql_constraint_type.downcase
        @cnstr_id = (
          "#{constraint_type} constraint%s on #{cnstr.qualified_table}" % [
            cnstr.name ? " " + cnstr.name : ''
          ]
        )
        
        if cnstr.name
          add_named_constraint_tests
        else
          add_unnamed_constraint_tests
        end
      end
      
      attr_reader :cnstr, :cnstr_id, :constraint_type
      
      def error_sql(s)
        @error_sql_proc.call(s)
      end
      
      def add_named_constraint_tests
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.key_constraints kc
              INNER JOIN sys.schemas s ON kc.schema_id = s.schema_id
              INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
              INNER JOIN sys.indexes i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
            }
            puts "WHERE s.name = %s" do
              puts strlit(unquoted_identifier cnstr.schema)
            end
            puts "AND t.name = %s" do
              puts strlit(unquoted_identifier cnstr.table)
            end
            puts "AND kc.name = %s" do
              puts strlit(unquoted_identifier cnstr.name)
            end
          end
          puts "BEGIN"
          indented {
            puts error_sql "#{cnstr_id.capitalize} does not exist."
          }
          puts "END ELSE BEGIN"
          indented {
            # Check that this constraint covers the correct fields, noting
            # that the constraint doesn't exist if cnstr.name.nil? or that
            # it doesn't have the expected fields, otherwise.
            declare_column_sequence_cursor_with_conditions {
              puts dedent %Q{
                INNER JOIN sys.schemas s ON kc.schema_id = s.schema_id
                INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
              }
              puts "WHERE s.name = %s" do
                puts strlit(unquoted_identifier cnstr.schema)
              end
              puts "AND t.name = %s" do
                puts strlit(unquoted_identifier cnstr.table)
              end
              puts "AND kc.name = %s" do
                puts strlit(unquoted_identifier cnstr.name)
              end
            }
            
            cnstr.columns.each do |index_column|
              add_column_sequence_test(index_column) do |error_message|
                puts error_sql error_message
              end
            end
            
            check_column_sequence_end
          }
          puts "END;"
        }
      end
      
      def add_unnamed_constraint_tests
        dsl {
          puts dedent %Q{
            DECLARE @constraint_id INT;
            
            DECLARE constraint_cursor CURSOR FOR
            SELECT kc.object_id
            FROM sys.key_constraints kc
            INNER JOIN sys.schemas s ON kc.schema_id = s.schema_id
            INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
          }
          puts "WHERE s.name = %s" do
            puts strlit(unquoted_identifier cnstr.schema)
          end
          puts "AND t.name = %s" do
            puts strlit(unquoted_identifier cnstr.table)
          end
          puts ";"
          puts "OPEN constraint_cursor;"
          
          puts dedent %Q{
            DECLARE @constraint_found BIT, @constraint_match_error BIT;
            SET @constraint_found = 0;
            FETCH NEXT FROM constraint_cursor INTO @constraint_id;
            WHILE @@FETCH_STATUS = 0 AND @constraint_found = 0
            BEGIN
          }
          indented {
            puts "SET @constraint_match_error = 0;"
            declare_column_sequence_cursor_with_conditions {
              puts "WHERE kc.object_id = @constraint_id"
            }
            
            cnstr.columns.each do |index_column|
              add_column_sequence_test(index_column) do |error_message|
                puts "SET @constraint_match_error = 1;"
              end
            end
            
            check_column_sequence_end
            
            puts %Q{
              IF @constraint_match_error = 0
              BEGIN
                SET @constraint_found = 1;
              END;
            }
          }
          puts "END;"
          puts dedent %Q{
            CLOSE constraint_cursor;
            DEALLOCATE constraint_cursor;
            
            IF @constraint_found = 0
          }
          puts "BEGIN".."END;" do
            puts error_sql "Expected #{cnstr_id} does not exist."
          end
        }
      end
      
      def declare_column_sequence_cursor_with_conditions
        dsl {
          puts dedent %Q{
            DECLARE @column_name SYSNAME, @column_sorted_descending BIT;
            DECLARE column_cursor CURSOR FOR
            SELECT c.name, ic.is_descending_key
            FROM sys.key_constraints kc
            INNER JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
          }
          yield
          puts "ORDER BY ic.index_column_id;"
          puts "OPEN column_cursor;"
        }
      end
      
      def check_column_sequence_end
        dsl {
          puts dedent %Q{
            FETCH NEXT FROM column_cursor INTO @column_name, @column_sorted_descending;
            IF @@FETCH_STATUS = 0
          }
          puts "BEGIN".."END;" do
            puts error_sql "#{cnstr_id.capitalize} has one or more unexpected columns."
          end
          puts "CLOSE column_cursor;"
          puts "DEALLOCATE column_cursor;"
        }
      end
      
      def add_column_sequence_test(index_column)
        dsl {
          puts %Q{
            FETCH NEXT FROM column_cursor INTO @column_name, @column_sorted_descending;
            IF @@FETCH_STATUS <> 0
          }
          puts "BEGIN"
          indented {
            yield "Column #{index_column.name} not found where expected in #{cnstr_id}."
          }
          puts "END ELSE IF NOT (%s)" do
            puts "@column_name = %s" do
              puts strlit(unquoted_identifier index_column.name)
            end
          end
          puts "BEGIN"
          indented {
            yield "Other column found where #{index_column.name} expected in #{cnstr_id}."
          }
          puts "END ELSE IF NOT (%s)" do
            puts bit_test("@column_sorted_descending", index_column.direction == :descending)
          end
          puts "BEGIN"
          indented {
            yield "Column #{index_column.name} should be sorted #{index_column.direction} in #{cnstr_id}."
          }
          puts "END;"
        }
      end
    end
    
    def check_primary_key_and_unique_constraints_sql
      db_expectations.pku_constraints.map do |cnstr|
        KeylikeConstraintAdoptionChecks.new(cnstr, method(:adoption_error_sql)).to_s
      end # Do not join -- each needs a separate batch (they use variables)
    end
    
    class ForeignKeyAdoptionChecks < IndentedStringBuilder
      include SqlStringManipulators
      
      def initialize(keys, error_sql_proc)
        super()
        
        @error_sql_proc = error_sql_proc
        @named_keys = keys.reject {|k| k.unnamed?}
        @unnamed_keys = keys.select {|k| k.unnamed?}
        
        add_named_key_tests
        add_unnamed_key_tests
      end
      
      attr_reader :named_keys, :unnamed_keys
      
      def error_sql(s)
        @error_sql_proc.call(s)
      end
      
      def add_named_key_tests
        table = 'expected_named_foreign_keys'
        dsl {
          # Create a temporary table
          puts dedent %Q{
            CREATE TABLE [xmigra].[#{table}] (
              [name] NVARCHAR(150) NOT NULL,
              [position] INTEGER NOT NULL,
              [from_table] NVARCHAR(300) NOT NULL,
              [from_column] NVARCHAR(150) NOT NULL,
              [to_table] NVARCHAR(300) NOT NULL,
              [to_column] NVARCHAR(150) NOT NULL
            );
            GO
          }
          
          # Insert a record for each column linkage for each named foreign key
          named_keys.each do |fkey|
            fkey.links.each.with_index do |cols, i|
              values = [
                strlit(fkey.name),
                i + 1,
                strlit(fkey.qualified_table),
                strlit(cols[0]),
                strlit(fkey.references.join '.'),
                strlit(cols[1])
              ]
              puts dedent(%Q{
                INSERT INTO [xmigra].[#{table}] (name, position, from_table, from_column, to_table, to_column)
                VALUES (%s);
              } % [values.join(', ')])
            end
          end
          
          # Write an adoption error for each missing/misdefined foreign key
          puts dedent %Q{
            WITH
              MissingLinks AS (
                SELECT
                  [name],
                  [position],
                  [from_table],
                  [from_column],
                  [to_table],
                  [to_column]
                FROM [xmigra].[#{table}]
                EXCEPT
                SELECT
                  QUOTENAME(fk.name) AS name,
                  RANK() OVER(PARTITION BY fk.object_id ORDER BY fkc.constraint_column_id ASC) AS position,
                  QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) AS from_table,
                  QUOTENAME(from_col.name) AS from_col,
                  QUOTENAME(rs.name) + N'.' + QUOTENAME(r.name) AS to_table,
                  QUOTENAME(to_col.name) AS to_col
                FROM sys.foreign_keys fk
                JOIN sys.tables t ON fk.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                JOIN sys.objects r ON fk.referenced_object_id = r.object_id
                JOIN sys.schemas rs ON r.schema_id = rs.schema_id
                JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                JOIN sys.columns from_col
                  ON fk.parent_object_id = from_col.object_id
                  AND fkc.parent_column_id = from_col.column_id
                JOIN sys.columns to_col
                  ON fk.referenced_object_id = to_col.object_id
                  AND fkc.referenced_column_id = to_col.column_id
              )
            INSERT INTO [xmigra].[adoption_errors] ([message])
            SELECT DISTINCT
              N'Constraint ' + ml.[name] + N' on ' + ml.[from_table] + N' (referencing' + ml.[to_table] + N') does not have the expected definition.'
            FROM MissingLinks ml;
            GO
          }
          
          # Drop the temporary table
          puts "DROP TABLE [xmigra].[#{table}];\nGO"
        }
      end
      
      def add_unnamed_key_tests
        table = 'expected_unnamed_foreign_keys'
        dsl {
          # Create a temporary table
          puts dedent %Q{
            CREATE TABLE [xmigra].[#{table}] (
              [position] INTEGER NOT NULL,
              [from_table] NVARCHAR(300) NOT NULL,
              [from_column] NVARCHAR(150) NOT NULL,
              [to_table] NVARCHAR(300) NOT NULL,
              [to_column] NVARCHAR(150) NOT NULL
            );
            GO
          }
          
          # Insert a record for each column linkage for each unnamed foreign key
          unnamed_keys.each do |fkey|
            fkey.links.each.with_index do |cols, i|
              values = [
                i + 1,
                strlit(fkey.qualified_table),
                strlit(cols[0]),
                strlit(fkey.references.join '.'),
                strlit(cols[1])
              ]
              puts dedent(%Q{
                INSERT INTO [xmigra].[#{table}] (position, from_table, from_column, to_table, to_column)
                VALUES (%s);
              } % [values.join(', ')])
            end
          end
          
          # Write an adoption error for each missing/misdefined key
          puts dedent %Q{
            WITH
              MissingLinks AS (
                SELECT
                  [position],
                  [from_table],
                  [from_column],
                  [to_table],
                  [to_column]
                FROM [xmigra].[#{table}]
                EXCEPT
                SELECT
                  RANK() OVER(PARTITION BY fk.object_id ORDER BY fkc.constraint_column_id ASC) AS position,
                  QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) AS from_table,
                  QUOTENAME(from_col.name) AS from_col,
                  QUOTENAME(rs.name) + N'.' + QUOTENAME(r.name) AS to_table,
                  QUOTENAME(to_col.name) AS to_col
                FROM sys.foreign_keys fk
                JOIN sys.tables t ON fk.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                JOIN sys.objects r ON fk.referenced_object_id = r.object_id
                JOIN sys.schemas rs ON r.schema_id = rs.schema_id
                JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                JOIN sys.columns from_col
                  ON fk.parent_object_id = from_col.object_id
                  AND fkc.parent_column_id = from_col.column_id
                JOIN sys.columns to_col
                  ON fk.referenced_object_id = to_col.object_id
                  AND fkc.referenced_column_id = to_col.column_id
              )
            INSERT INTO [xmigra].[adoption_errors] ([message])
            SELECT DISTINCT
              N'Expected constraint on ' + ml.[from_table] + N' (referencing ' + ml.[to_table] + N') not found.'
            FROM MissingLinks ml;
            GO
          }
          
          # Drop the temporary table
          puts "DROP TABLE [xmigra].[#{table}];\nGO"
        }
      end
    end
    
    def check_foreign_key_constraints_sql
      ForeignKeyAdoptionChecks.new(db_expectations.foreign_keys, method(:adoption_error_sql)).to_s
    end
    
    class CheckConstraintAdoptionChecks < IndentedStringBuilder
      include SqlStringManipulators
      
      def initialize(cnstr, error_sql_proc)
        super()
        
        @cnstr = cnstr
        @cnstr_id = "check constraint%s on #{cnstr.qualified_table}" % [
          cnstr.name ? cnstr.name + " " : ""
        ]
        @error_sql_proc = error_sql_proc
        
        @schema_name = unquoted_identifier cnstr.schema
        @table_name = unquoted_identifier cnstr.table
        @cnstr_name = unquoted_identifier(cnstr.name) if cnstr.name
        
        if cnstr.name
          add_named_constraint_tests
        else
          add_unnamed_constraint_tests
        end
      end
      
      attr_reader :cnstr, :cnstr_id, :schema_name, :table_name
      
      def error_sql(s)
        @error_sql_proc.call(s)
      end
      
      def add_named_constraint_tests
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.check_constraints cc
              INNER JOIN sys.tables t ON cc.object_id = t.object_id
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE s.name = #{strlit schema_name}
              AND t.name = #{strlit table_name}
              AND cc.name = #{strlit cnstr_name}
            }
          end
          puts "BEGIN"
          indented {
            puts error_sql "#{cnstr_id.capitalize} does not exist."
          }
          puts "END ELSE IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.check_constraints cc
              INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE s.name = #{strlit schema_name}
              AND t.name = #{strlit table_name}
              AND cc.name = #{strlit cnstr_name}
              AND cc.definition = #{strlit cnstr.definition}
            }
          end
          puts "BEGIN"
          indented {
            puts error_sql "#{cnstr_id.capitalize} does not have expected definition."
          }
          puts "END;"
        }
      end
      
      def add_unnamed_constraint_tests
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT cc.object_id
              FROM sys.check_constraints cc
              INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE s.name = #{strlit schema_name}
              AND t.name = #{strlit table_name}
              AND cc.definition = #{strlit cnstr.definition}
            }
          end
          puts "BEGIN".."END;" do
            puts error_sql "Expected #{cnstr_id} does not exist."
          end
        }
      end
    end
    
    def check_check_constraints_sql
      db_expectations.check_constraints.map do |cnstr|
        CheckConstraintAdoptionChecks.new(cnstr, method(:adoption_error_sql)).to_s
      end # Do not join -- each needs a separate batch (they use variables)
    end
    
    class IndexAdoptionChecks < IndentedStringBuilder
      include SqlStringManipulators
      
      def initialize(index, error_sql_proc)
        super()
        
        @index = index
        @error_sql_proc = error_sql_proc
        
        @index_id = "index #{@index.name} on #{@index.qualified_relation}"
        
        dsl {
          puts "DECLARE @relation_id INT, @index_id INT;"
          puts dedent %Q{
            SELECT @relation_id = i.object_id, @index_id = i.index_id
            FROM sys.indexes i
            JOIN sys.objects rel ON i.object_id = rel.object_id
            JOIN sys.schemas s ON rel.schema_id = s.schema_id
            WHERE s.name = #{strlit(unquoted_identifier index.schema)}
            AND rel.name = #{strlit(unquoted_identifier index.relation)}
            AND i.name = #{strlit(unquoted_identifier index.name)}
          }
          puts "IF @index_id IS NULL"
          puts "BEGIN"
          indented {
            puts error_sql "#{index_id.capitalize} does not exist."
          }
          puts "END ELSE BEGIN"
          indented {
            add_index_property_checks
          }
          puts "END"
        }
      end
      
      attr_reader :index, :index_id
      
      def error_sql(s)
        @error_sql_proc.call(s)
      end
      
      def add_index_property_checks
        dsl {
          puts property_verification("is_unique", index.unique?, "be unique")
          puts property_verification("ignore_dup_key", index.ignore_duplicates?, "ignore duplicate keys")
          
          # Key columns
          QueryCursor.new(
            dedent(%Q{
              SELECT c.column_name, ic.is_descending_key
              FROM sys.index_columns ic
              JOIN sys.columns c 
                ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
              WHERE ic.object_id = @relation_id
              AND ic.index_id = @index_id
              AND ic.key_ordinal >= 1
              ORDER BY ic.key_ordinal
            }),
            "@column_name SYSNAME, @is_sorted_descending BIT",
            output_to: self
          ).expectations(
            on_extra: ->{puts error_sql "#{index_id.capitalize} has one or more unexpected key columns."}
          ) do |test|
            index.columns.each.with_index do |column, i|
              test.row(
                on_missing: ->{puts error_sql "#{index_id.capitalize} is missing expected column #{column.name}."}
              ) {
                puts "IF QUOTENAME(@column_name) <> #{strlit column.name}"
                puts "BEGIN"
                indented {
                  puts error_sql "Expected #{column.name} as column #{i + 1} in #{index_id}."
                }
                puts "END ELSE IF #{bit_test('@is_sorted_descending', column.direction == :descending)}"
                indented {
                  puts error_sql "Expected #{column.name} to be sorted #{column.direction} in #{index_id}."
                }
                puts "END;"
              }
            end
          end
          
          # Included columns
          included_column_names = index.included_columns.map {|c| c.name}
          puts "IF (%s) < #{included_column_names.length}" do
            puts dedent %Q{
              SELECT COUNT(*) FROM sys.index_columns ic
              JOIN sys.columns c ON ic.object_id = c.object_id AND ic.index_id = c.index_id
              WHERE ic.object_id = @relation_id
              AND ic.index_id = @index_id
              AND ic.key_ordinal = 0
              AND QUOTENAME(c.name) IN (#{included_column_names.map {|s| strlit s}.join(', ')})
            }
          end
          puts "BEGIN".."END" do
            puts error_sql "#{index_id.capitalize} is missing one or more expected included columns."
          end
        }
        
        add_spatial_property_checks(index) if index.spatial_index_geometry
      end
      
      def index_property_check(expectation, expectation_desc)
        %Q{
          IF NOT EXIST (
            SELECT * FROM sys.indexes i
            WHERE i.object_id = @relation_id
            AND i.index_id = @index_id
            AND i.#{expectation}
          )
          BEGIN
            #{error_sql "#{@index_id.capitalize} should #{expectation_desc}."}
          END;
        }.strip.gsub(/\s+/, ' ')
      end
      
      def property_verification(f, v, d)
        index_property_check(bit_test(f, v), boolean_desc(v, d))
      end
    end
    
    def adopt_indexes_sql
      db_expectations.indexes.map do |index|
        index_builder = @xn_builder.indexes[index.name]
        IndexAdoptionChecks.new(index, method(:adoption_error_sql)).to_s +
        "\nINSERT INTO [xmigra].[indexes] ([IndexID], [name]) VALUES (%s, %s);" % [
          index_builder.id, index_builder.name
        ].map {|s| strlit(s)}
      end
    end
    
    class StatisticsAdoptionChecks < IndentedStringBuilder
      include SqlStringManipulators
      
      def initialize(statistics, error_sql_proc)
        super()
        
        @statistics = statistics
        @error_sql_proc = error_sql_proc
        
        @stats_id = "statistics #{statistics.name} on #{statistics.qualified_relation}"
        
        dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.stats so
              INNER JOIN sys.objects rel ON so.object_id = rel.object_id
              INNER JOIN sys.schemas s ON rel.schema_id = s.schema_id
              WHERE s.name = #{strlit(unquoted_identifier statistics.schema)}
              AND rel.name = #{strlit(unquoted_identifier statistics.relation)}
              AND so.name = #{strlit(unquoted_identifier statistics.name)}
            }
          end
          puts "BEGIN"
          indented {
            puts error_sql "#{stats_id.capitalize} does not exist."
          }
          puts "END ELSE BEGIN"
          indented {
            # Check column sequence
            QueryCursor.new(
              dedent(%Q{
                SELECT c.name
                FROM sys.stats so
                JOIN sys.stats_columns sc
                  ON so.object_id = sc.object_id
                  AND so.stats_id = sc.stats_id
                JOIN sys.columns c
                  ON sc.object_id = c.object_id
                  AND sc.column_id = c.column_id
                JOIN sys.objects rel ON so.object_id = rel.object_id
                JOIN sys.schemas s ON rel.schema_id = s.schema_id
                WHERE s.name = #{strlit(unquoted_identifier statistics.schema)}
                AND rel.name = #{strlit(unquoted_identifier statistics.relation)}
                AND so.name = #{strlit(unquoted_identifier statistics.name)}
                ORDER BY sc.stats_column_id
              }),
              "@column_name SYSNAME",
              output_to: self
            ).expectations(
              on_extra: ->{puts error_sql "#{stats_id.capitalize} has one or more unexpected columns."},
            ) do |test|
              statistics.columns.each.with_index do |col_name, i|
                test.row(
                  on_missing: ->{puts error_sql "#{stats_id.capitalize} is missing #{col_name}."},
                ) {
                  puts "IF QUOTENAME(@column_name) <> #{strlit col_name}"
                  puts "BEGIN".."END" do
                    puts error_sql "Expected #{col_name} as column #{i + 1} of #{stats_id}."
                  end
                }
              end
            end
          }
          puts "END;"
        }
      end
      
      attr_reader :stats_id
      
      def error_sql(s)
        @error_sql_proc.call(s)
      end
    end
    
    def adopt_statistics_sql
      db_expectations.statistics.map do |statistics|
        StatisticsAdoptionChecks.new(statistics, method(:adoption_error_sql)).to_s +
        "\nINSERT INTO [xmigra].[statistics] ([Name], [Columns]) VALUES (%s, %s);" % [
          statistics.name,
          statistics.columns.join(', ')
        ].map {|s| strlit(s)}
      end
    end
    
    def access_object_adoption_sql(type, qualified_name)
      "INSERT INTO [xmigra].[access_objects] ([type], [name]) VALUES (N'#{type}', #{strlit qualified_name});"
    end
    
    def definition_matches_by_hash(expr, definition)
      "HASHBYTES('md5', #{expr}) = 0x#{Digest::MD5.hexdigest definition.gsub("\n", "\r\n").encode('UTF-16LE')}"
    end
    
    def adopt_views_sql
      db_expectations.views.map do |view|
        IndentedStringBuilder.dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.views v
              JOIN sys.schemas s ON v.schema_id = s.schema_id
              WHERE s.name = #{strlit(unquoted_identifier view.schema)}
              AND v.name = #{strlit(unquoted_identifier view.name)}
            }
          end
          puts "BEGIN"
          indented do
            puts adoption_error_sql "View #{view.qualified_name} does not exist."
          end
          puts "END ELSE IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.views v
              JOIN sys.schemas s ON v.schema_id = s.schema_id
              JOIN sys.sql_modules sql ON v.object_id = sql.object_id
              WHERE s.name = #{strlit(unquoted_identifier view.schema)}
              AND v.name = #{strlit(unquoted_identifier view.name)}
              AND #{definition_matches_by_hash 'sql.definition', view.definition}
            }
          end
          puts "BEGIN"
          indented {
            puts adoption_error_sql "View #{view.qualified_name} does not have the expected definition."
          }
          puts "END;"
          puts access_object_adoption_sql(:VIEW, view.qualified_name)
        }
      end
    end
    
    def adopt_stored_procedures_sql
      db_expectations.procedures.map do |sproc|
        IndentedStringBuilder.dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.procedures p
              JOIN sys.schemas s ON p.schema_id = s.schema_id
              WHERE s.name = #{strlit(unquoted_identifier sproc.schema)}
              AND p.name = #{strlit(unquoted_identifier sproc.name)}
            }
          end
          puts "BEGIN"
          indented {
            puts adoption_error_sql "Stored procedure #{sproc.qualified_name} does not exist."
          }
          puts "END ELSE IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.procedures p
              JOIN sys.schemas s ON p.schema_id = s.schema_id
              JOIN sys.sql_modules sql ON p.object_id = sql.object_id
              WHERE s.name = #{strlit(unquoted_identifier sproc.schema)}
              AND p.name = #{strlit(unquoted_identifier sproc.name)}
              AND #{definition_matches_by_hash('sql.definition', sproc.definition)}
            }
          end
          puts "BEGIN"
          indented {
            puts adoption_error_sql "Stored procedure #{sproc.qualified_name} does not have the expected definition."
          }
          puts "END;"
          puts access_object_adoption_sql(:PROCEDURE, sproc.qualified_name)
        }
      end
    end
    
    def adopt_udfs_sql
      db_expectations.udfs.map do |udf|
        IndentedStringBuilder.dsl {
          puts "IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.objects fn
              JOIN sys.schemas s ON fn.schema_id = s.schema_id
              WHERE s.name = #{strlit(unquoted_identifier udf.schema)}
              AND fn.name = #{strlit(unquoted_identifier udf.name)}
              AND fn.type IN ('FN', 'IF', 'TF')
            }
          end
          puts "BEGIN"
          indented {
            puts adoption_error_sql "Function #{udf.qualified_name} does not exist."
          }
          puts "END ELSE IF NOT EXISTS (%s)" do
            puts dedent %Q{
              SELECT * FROM sys.objects fn
              JOIN sys.schemas s ON fn.schema_id = s.schema_id
              JOIN sys.sql_modules sql ON fn.object_id = sql.object_id
              WHERE s.name = #{strlit(unquoted_identifier udf.schema)}
              AND fn.name = #{strlit(unquoted_identifier udf.name)}
              AND #{definition_matches_by_hash 'sql.definition', udf.definition}
            }
          end
          puts "BEGIN"
          indented {
            puts adoption_error_sql "Function #{udf.qualified_name} does not have the expected definition."
          }
          puts "END;"
          puts access_object_adoption_sql(:FUNCTION, udf.qualified_name)
        }
      end
    end
    
    def adopt_permissions_sql
      table = 'expected_permissions'
      [
        # Create a temporary table
        dedent(%Q{
          CREATE TABLE [xmigra].[#{table}] (
            [state] CHAR(1) NOT NULL,
            [subject] NVARCHAR(150) NOT NULL,
            [permission] NVARCHAR(128) NOT NULL,
            [object_type] NVARCHAR(25) NOT NULL,
            [object_schema] NVARCHAR(150) NULL,
            [object] NVARCHAR(150) NULL,
            [column] NVARCHAR(150) NULL
          );
        }),
        # Insert permission rows into the table
        [].tap do |inserts|
          db_expectations.permissions.each do |pg|
            pg.permissions.each do |pmsn|
              state = case pg.action[0].downcase
              when 'g' then pmsn.grant_option? ? 'W' : 'G'
              else pg.action[0].upcase
              end
              nls = ->(s) {s.nil? ? 'NULL' : strlit(s)}
              row_values = [state, pg.subject, pmsn.name] + pmsn.object_id_parts
              inserts << dedent(%Q{
                INSERT INTO [xmigra].[#{table}] (state, subject, permission, object_type, object_schema, object, [column])
                VALUES (%s);
              } % row_values.map(&nls).join(', '))
            end
          end
        end.join("\n"),
        # Write an adoption error for each missing permission
        dedent(%Q{
          WITH
            PermissionTarget AS (
              SELECT
                0 AS "class",
                0 AS major_id,
                0 AS minor_id,
                'DATABASE' AS "class_desc",
                NULL AS "class_specifier",
                NULL AS "schema_name",
                NULL AS "object_name",
                NULL AS "column_name"
              UNION
              SELECT
                1,
                o.object_id,
                0,
                'OBJECT',
                NULL,
                s.name,
                o.name,
                NULL
              FROM sys.objects o
              JOIN sys.schemas s ON o.schema_id = s.schema_id
              UNION
              SELECT
                1,
                c.object_id,
                c.column_id,
                'COLUMN',
                NULL,
                s.name,
                o.name,
                c.name
              FROM sys.columns c
              JOIN sys.objects o ON c.object_id = o.object_id
              JOIN sys.schemas s ON o.schema_id = s.schema_id
              UNION
              SELECT
                3,
                s.schema_id,
                0,
                'SCHEMA',
                'SCHEMA',
                NULL,
                s.name,
                NULL
              FROM sys.schemas s
              UNION
              SELECT
                4,               -- class
                r.principal_id,  -- major_id
                0,               -- minor_id
                'ROLE',          -- class description
                'ROLE',          -- class specifier
                NULL,            -- schema_name
                r.name,          -- object_name
                NULL             -- column_name
              FROM sys.database_principals r
              WHERE r.type = 'R'
              UNION
              SELECT
                5,               -- class
                a.assembly_id,   -- major_id
                0,               -- minor_id
                'ASSEMBLY',      -- class description
                'ASSEMBLY',      -- class specifier
                NULL,            -- schema_name
                a.name,          -- object_name
                NULL             -- column_name
              FROM sys.assemblies a
              UNION
              SELECT
                6,               -- class
                t.user_type_id,  -- major_id
                0,               -- minor_id
                'TYPE',          -- class description
                'TYPE',          -- class specifier
                s.name,          -- schema_name
                t.name,          -- object_name
                NULL             -- column_name
              FROM sys.types t
              JOIN sys.schemas s ON t.schema_id = s.schema_id
              UNION
              SELECT
                10,                        -- class
                xsc.xml_collection_id,     -- major_id
                0,                         -- minor_id
                'XML_SCHEMA_COLLECTION',   -- class description
                'XML SCHEMA COLLECTION',   -- class specifier
                s.name,                    -- schema_name
                xsc.name,                  -- object_name
                NULL                       -- column_name
              FROM sys.xml_schema_collections xsc
              JOIN sys.schemas s ON xsc.schema_id = s.schema_id
            ),
            Permissions AS (
              SELECT
                p.state,
                QUOTENAME(pi.name) AS "subject",
                p.permission_name AS "permission",
                t.class_desc AS "object_type",
                QUOTENAME(t.schema_name) AS "object_schema",
                QUOTENAME(t.object_name) AS "object",
                QUOTENAME(t.column_name) AS "column"
              FROM sys.database_permissions p
              JOIN sys.database_principals pi ON p.grantee_principal_id = pi.principal_id
              JOIN PermissionTarget t 
                ON p.class = t.class 
                AND p.major_id = t.major_id
                AND p.minor_id = t.minor_id
              LEFT JOIN sys.database_principals grantor ON p.grantor_principal_id = grantor.principal_id
              AND (p.class <> 4 OR (
                SELECT dp.type FROM sys.database_principals dp
                WHERE dp.principal_id = p.major_id
              ) = 'R')
              AND (p.class <> 1 OR p.major_id IN (
                SELECT o.object_id FROM sys.objects o
              ))
            )
          INSERT INTO [xmigra].[adoption_errors] ([message])
          SELECT
            e.permission + N' is ' +
            CASE e.state
            WHEN 'G' THEN 
              CASE (
                SELECT p.state
                FROM Permissions p
                WHERE p.subject = e.subject
                AND p.permission = e.permission
                AND p.object_type = e.object_type
                AND COALESCE(p.object_schema, N'.') = COALESCE(e.object_schema, N'.')
                AND COALESCE(p.object, N'.') = COALESCE(e.object, N'.')
                AND COALESCE(p.[column], N'.') = COALESCE(e.[column], N'.')
              )
              WHEN 'W' THEN 'GRANTed with (unexpected) grant option to '
              ELSE N'not GRANTed to '
              END
            WHEN 'W' THEN N'not GRANTed (with grant option) to '
            WHEN 'D' THEN N'not DENYed to '
            WHEN 'R' THEN N'not REVOKEd from '
            END + e.subject + N' on ' + e.object_type +
            CASE 
            WHEN e.object_schema IS NULL THEN N''
            ELSE e.object_schema + N'.'
            END +
            CASE
            WHEN e.object IS NULL THEN N''
            ELSE e.object
            END +
            CASE
            WHEN e.[column] IS NULL THEN N''
            ELSE N' (' + e.[column] + N')'
            END + N'.'
          FROM (
            SELECT state, subject, permission, object_type, object_schema, object, [column]
            FROM [xmigra].[#{table}]
            EXCEPT
            SELECT 
              state COLLATE SQL_Latin1_General_CP1_CI_AS, 
              subject, 
              permission COLLATE SQL_Latin1_General_CP1_CI_AS, 
              object_type, 
              object_schema, 
              object, 
              [column]
            FROM Permissions
          ) e
        }),
        # Record adopted permissions
        db_expectations.permissions.map do |pg|
          pg.regular_permissions.map do |pmsn|
            "EXEC [xmigra].[ip_prepare_revoke] #{[pmsn.name, pmsn.target, pg.subject].map {|s| strlit(unquoted_identifier s)}.join(', ')};"
          end
        end.flatten.join("\n"),
        # Drop the temporary table
        "DROP TABLE [xmigra].[#{table}];",
      ]
    end
    
    def write_version_bridge_record_sql
      dedent %Q{
        INSERT INTO [xmigra].[applied] ([MigrationID], [VersionBridgeMark], [Description])
        VALUES (#{strlit @xn_builder.migrations.last.id}, 1, N'Adoption of existing structure.');
      }
    end
  end
end
