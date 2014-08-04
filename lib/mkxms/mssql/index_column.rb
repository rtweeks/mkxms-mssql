module Mkxms; end
module Mkxms::Mssql; end

Mkxms::Mssql.const_set(
  :IndexColumn,
  Struct.new(:name, :direction) do
    def to_sql
      "#{name} #{direction == :descending ? 'DESC' : 'ASC'}"
    end
  end
)
