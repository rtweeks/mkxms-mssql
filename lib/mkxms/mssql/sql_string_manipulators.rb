require 'xmigra'

module Mkxms; end

module Mkxms::Mssql
  module SqlStringManipulators
    MSSQL = XMigra::MSSQLSpecifics
    
    def dedent(s)
      margin = nil
      s.lines.map do |l|
        case 
        when margin.nil? && l =~ /^ *$/
          l
        when margin.nil?
          margin = /^ */.match(l)[0].length
          l[margin..-1]
        when s =~/^\s*$/
          l[margin..-1]
        else
          /^(?: *)(.*)/.match(l)[1]
        end
      end.tap do |lines|
        lines.shift if lines.first == "\n"
      end.join('')
    end
    
    def stresc(s)
      s.gsub("'", "''")
    end
    
    def strlit(s)
      MSSQL.string_literal(s)
    end
    
    def unquoted_identifier(s)
      MSSQL.strip_identifier_quoting(s)
    end
    
    def bit_test(expr, expected)
      "#{expr} = #{expected ? 1 : 0}"
    end
    
    def boolean_desc(_is, s)
      (_is ? '' : 'not ') + s
    end
  end
end
