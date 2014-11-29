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
    end
  end
end
