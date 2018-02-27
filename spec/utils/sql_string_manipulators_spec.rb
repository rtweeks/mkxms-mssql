require 'mkxms/mssql/sql_string_manipulators'

describe Mkxms::Mssql::SqlStringManipulators do
  module T
    extend Mkxms::Mssql::SqlStringManipulators
  end
  
  context "dedenting" do
    it "handles a string with no newlines" do
      expect(T.dedent("foo")).to eq("foo")
    end
    
    it "handles a string with a all lines at a constant indent" do
      s = T.dedent %Q{
        Jack and Jill went up the hill
        to fetch a pail of water
      }
      expect(s.lines.size).to eq(2)
      expect(s.lines[0]).not_to start_with(' ')
    end
    
    it "handles nested indentation" do
      s = T.dedent %Q{
        one
          two
        one
          two
      }
      expect(s.lines.size).to eq(4)
      expect(s.lines[0]).not_to start_with(' ')
      expect(s.lines[1]).to start_with('  t')
    end
    
    it "allows leading blank lines" do
      s = T.dedent %Q{
        
        after the break
      }
      expect(s.lines[0]).to match(/^\s*$/)
    end
    
    it "allows trailing blank lines" do
      s = T.dedent %Q{
        before the break
        
      }
      expect(s.lines[1]).to match(/^\s*$/)
    end
    
    it "allows internal blank lines" do
      s = T.dedent %Q{
        before
        
        after
      }
      expect(s.lines[1]).to eq("\n")
    end
  end
end
