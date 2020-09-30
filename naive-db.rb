#! /usr/bin/env ruby

### ENGINE
##########

# To make the API easier, we will abuse global state.
# The database file(s) are stored in a global variable.
$tables = {}

# Until we have an official table schema
# We will store the row length as a constant
ROW_LENGTH = 48
require "byebug"

class IO
  TAIL_BUF_LENGTH = 1 << 12 # Standard page size
  def last_line
    seek(-TAIL_BUF_LENGTH, SEEK_END)
    buf = ""
    while buf.count("\n") <= 2
      buf = read(TAIL_BUF_LENGTH, SEEK_CUR)
      seek(-2*TAIL_BUF_LENGTH, SEEK_CUR)
    end
    buf.split("\n").last
  rescue SystemCallError
    seek(0, SEEK_SET)
    read().split("\n").last
  end
end

# Initialize table
def connect(table_name)
  file_path = "#{table_name}.csv"
  mode = File.exists?(file_path) ? "r+" : "w+"
  file = File.open(file_path, mode)
  raise "Database Corrupt" if file.size() % ROW_LENGTH != 0
  $tables[table_name] = file
end

def close()
  $tables.values.each { |file| file.close() }
end

def binary_search(file, match_id)
  row_count = file.size() / ROW_LENGTH
  i = 0
  j = row_count - 1

  while i <= j do
    middle = (i + j) / 2
    file_offset = middle * ROW_LENGTH
    file.seek(file_offset, File::SEEK_SET)
    row = file.read(ROW_LENGTH)
    row_id = row[0...10].to_i

    if row_id == match_id
      return row
    elsif row_id < match_id
      i = middle + 1
    else
      j = middle - 1
    end
  end

  return nil
end

def read(table, id)
  file = $tables[table]
  raw_row = binary_search(file, id)
  raw_row.nil? ? [] : deserialize(raw_row)
end

# For now, we assume that write does not include id
def write(table, row)
  file = $tables[table]

  # By default, set the ID to the last index + 1
  # This can potentially re-use a deleted ID
  # But it cannot (yet) create two rows with the same ID
  id = file.size() == 0 ?
    1 :
    file.last_line.split(",").first.to_i + 1

  # Add the id to the beginning of the row
  row.unshift(id)

  file.puts(serialize(row))

  id
end

# For simplicity, we will completely re-write the table
def delete(table, id)
  file = $tables[table]
  raw_row = binary_search(file, id)
  row_id = raw_row[0...10].to_i

  # Read the data after the current row
  cur_size = file.size()
  file_offset = row_id * ROW_LENGTH
  file.seek(file_offset, File::SEEK_SET)
  file_end = file.read(file.size() - file_offset)

  # Overwrite file starting at current row
  file.seek((row_id - 1) * ROW_LENGTH, File::SEEK_SET)
  file.write(file_end)

  file.truncate(cur_size - ROW_LENGTH) # Shrink size of file by one row

  true
end

def deserialize(raw_row)
  raw_row
    .split(",")
    .map(&:strip)
    .map { |val| atom(val) }
end

def quote(str)
  quotes = ["\"", "'"]
  return str if quotes.include?(str[0]) && quotes.include?(str[-1])
  "\"#{str}\""
end

# Turn row to CSV
def serialize(row)
  row
    .map { |c| db_format(c) }
    .join(",")
end

def db_format(atom)
  if atom.is_a?(Integer)
    atom.to_s.rjust(10, "0")
  elsif atom.is_a?(Float)
    atom.round(4).to_s.rjust(15, "0")
  elsif atom.is_a?(String)
    "\"#{atom[0...18]}\"".ljust(20, " ")
  else
    raise "Unexpected Atom `#{atom}` of type `#{atom.class}`"
  end
end

### PARSER
##########

def atom(str) # -> Mixed
  begin
    Integer(str)
  rescue
    begin
      Float(str)
    rescue
      quotes = ["\"", "'"]
      if quotes.include?(str[0])
        str[1...str.rindex(str[0])]
      else
        str.to_sym
      end
    end
  end
end


def extract_table_name(query_str) # -> String
  query_str
    .match(/(FROM|INTO)\s+([.\w]*)/)
    .to_a
    .last
end

def extract_row_id(query_str) # -> Integer
  query_str
    .match(/WHERE\s+id\s*=\s*(\d+)/)
    .to_a
    .last
    .to_i
end

def extract_row(query_str) # -> Row
  raw_row = query_str
    .match(/VALUES\s*\(([^)]*)\)/)
    .to_a
    .last
  deserialize(raw_row)
end

def get_command(query_str) # -> Symbol
  query_str
    .strip
    .split(" ", 2)
    .first
    .downcase
    .to_sym
end


def parse_and_execute(query_str) # -> Mixed
  table_name = extract_table_name(query_str)

  case get_command(query_str)
  when :select
    row_id = extract_row_id(query_str)
    read(table_name, row_id)
  when :delete
    row_id = extract_row_id(query_str)
    delete(table_name, row_id)
  when :insert
    row = extract_row(query_str)
    write(table_name, row)
  end
end

### INTERFACE
#############

# We will process meta-commands separately
# Meta-commands start with a "."
# For now we will support only ".exit" - to escape the REPL.
def process_meta_command(command) # -> String
  if command == :exit
    puts "EXIT..."
    close()
    exit
  end
  ""
end

# Main REPL loop
if $PROGRAM_NAME == __FILE__
  connect("debits")
  while (true) do
    print(" > ")
    query_str = gets.strip

    if query_str[0] == "."
      resp =  process_meta_command(query_str[1..-1].to_sym)
    else
      resp = parse_and_execute(query_str)
    end

    puts resp
  end
end
