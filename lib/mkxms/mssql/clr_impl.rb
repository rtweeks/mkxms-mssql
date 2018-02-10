
module Mkxms; end

module Mkxms::Mssql
  ClrMethod = Struct.new(:assembly, :asm_class, :method) do
    def full_specifier
      to_a.join('.')
    end
  end
end
