require_relative "naive-db.rb"

def serialize_table(table)
  test_data.map { |row| serialize(row) }.join("\n") + "\n"
end

describe "naive-db" do
  before(:each) do
    File.delete(db_fname) if File.exists?(db_fname)
    File.write(db_fname, test_text)
    connect(db_name)
  end

  after(:each) do
    close()
  end

  let(:test_data) do
    [
      [1, "test", 100],
      [2, "test", 200],
      [3, "test", 300],
      [4, "foo", 808],
      [5, "foo", 25.50]
    ]
  end

  let(:test_text) { serialize_table(test_data) }

  let(:db_name) { ".test" }
  let(:db_fname) { "#{db_name}.csv" }

  subject { parse_and_execute(query) }

  context "basic functionality" do
    context "SELECT" do
      context "functions" do
        let(:query) { "SELECT * FROM .test WHERE id = 1;" }
        it "SELECTS" do
          expect(subject).to eq(test_data[0])
        end
      end

      context "negative ID" do
        let(:query) { "SELECT * FROM .test WHERE id = -2;" }
        it "fails gracefully" do
          expect(subject).to eq([])
        end
      end

      context "non-existent ID" do
        let(:query) { "SELECT * FROM .test  WHERE id = 7;" }
        it "fails gracefully"  do
          expect(subject).to eq([])
        end
      end
    end

    context "INSERT" do
      let(:query) { "INSERT INTO .test VALUES ('baz', 77.77);" }
      let(:inserted_row) { [6, "baz", 77.77] }

      context "functions" do
        it "INSERTS" do
          expect(subject).to eq(6)
          close()
          row = deserialize(File.readlines(db_fname).last)
          expect(row).to eq(inserted_row)
        end
      end

      context "persists" do
        it "persists" do
          subject
          resp = parse_and_execute("SELECT * FROM .test WHERE id = 6;")
          expect(resp).to eq(inserted_row)
        end
      end
    end

    context "DELETE" do
      let(:query) { "DELETE FROM .test WHERE id = 3;" }
      context "functions" do
         let(:deleted_table) do
           dup_data = test_data.dup
           dup_data.delete_at(2)
           dup_data
         end
         let(:deleted_data) { serialize_table(deleted_table) }

         it "DELETS" do
           close()
           expect(File.read(db_fname)).to eq(deleted_data)
        end
      end

      context "persists" do
        it "persists" do
          subject
          resp = parse_and_execute("SELECT * FROM .test WHERE id = 3;")
          expect(resp).to eq([])
        end
      end
    end
  end
end

