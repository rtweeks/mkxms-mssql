require 'mkxms/mssql/query_cursor'

describe Mkxms::Mssql::QueryCursor do
  before :each do
    allow(described_class).to receive(:generated_cursor_name).and_return("test_cursor")
  end
  
  let(:sql) {StringIO.new}
  
  it "provides an basic cursor loop" do
    described_class.new(
      "SELECT schema_id, name FROM sys.schemas",
      "@schema_id INT, @schema_name SYSNAME",
      output_to: sql,
    ).each_row do
      sql.puts "  -- Handle a row"
    end
    
    expect(sql.string).to eql("DECLARE @schema_id INT, @schema_name SYSNAME;\nDECLARE test_cursor CURSOR LOCAL FOR\nSELECT schema_id, name FROM sys.schemas;\nOPEN test_cursor;\nFETCH NEXT FROM test_cursor INTO @schema_id, @schema_name;\nWHILE @@FETCH_STATUS = 0\nBEGIN\n  -- Handle a row\n  FETCH NEXT FROM test_cursor INTO @schema_id, @schema_name;\nEND;\n")
  end
  
  it "provides a expectation loop" do
    columns = {
      foo: :ascending,
      bar: :ascending,
      baz: :descending,
    }
    described_class.new(
      %Q{
        SELECT c.name, ic.is_descending_key
        FROM sys.index_columns ic
        JOIN sys.columns c
          ON ic.object_id = c.object_id
          AND ic.column_id = c.column_id
        WHERE ic.object_id = @relation_id
        AND ic.index_id = @index_id
        AND ic.key_ordinal >= 1
        ORDER BY ic.key_ordinal
      }.gsub(/\s+/, ' ').strip,
      "@column_name SYSNAME, @is_sorted_descending BIT",
      output_to: sql
    ).expectations(
      on_extra: ->{sql.puts "-- Handle extra entry/ies"}
    ) do |test|
      columns.each_pair do |name, direction|
        test.row(
          on_missing: ->{sql.puts "-- Handle missing entry"}
        ) {
          sql.puts "-- Test column name is #{name}."
          sql.puts "-- Test is #{direction}."
        }
      end
    end
    
    expect(sql.string).to eql("DECLARE @column_name SYSNAME, @is_sorted_descending BIT;\nDECLARE test_cursor CURSOR LOCAL FOR\nSELECT c.name, ic.is_descending_key FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE ic.object_id = @relation_id AND ic.index_id = @index_id AND ic.key_ordinal >= 1 ORDER BY ic.key_ordinal;\nOPEN test_cursor;\n\nFETCH NEXT FROM test_cursor INTO @column_name, @is_sorted_descending;\nIF @@FETCH_STATUS <> 0\nBEGIN\n-- Handle missing entry\nEND ELSE BEGIN\n-- Test column name is foo.\n-- Test is ascending.\nEND;\n\nFETCH NEXT FROM test_cursor INTO @column_name, @is_sorted_descending;\nIF @@FETCH_STATUS <> 0\nBEGIN\n-- Handle missing entry\nEND ELSE BEGIN\n-- Test column name is bar.\n-- Test is ascending.\nEND;\n\nFETCH NEXT FROM test_cursor INTO @column_name, @is_sorted_descending;\nIF @@FETCH_STATUS <> 0\nBEGIN\n-- Handle missing entry\nEND ELSE BEGIN\n-- Test column name is baz.\n-- Test is descending.\nEND;\nFETCH NEXT FROM test_cursor INTO @column_name, @is_sorted_descending;\nIF @@FETCH_STATUS = 0\nBEGIN\n-- Handle extra entry/ies\nEND;\nCLOSE  test_cursor; DEALLOCATE  test_cursor;\n")
  end
end
