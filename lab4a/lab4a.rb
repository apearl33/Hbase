# Your code goes here
# 1. Create a function called put_many() that creates a Put instance, adds any number of column - value pairs to it, and commits it to a table.

import 'org.apache.hadoop.hbase.client.HTable'
import 'org.apache.hadoop.hbase.client.Put'

def jbytes( *args )
    args.map { |arg| arg.to_s.to_java_bytes }
end

def put_many( table_name, row, column_values )
    table = HTable.new( @hbase.configuration, table_name )
    p = Put.new( *jbytes(row) )
    column_values.each do |v|
        column_family = v[0].split(":", -1)[0]
        column_qualifer = v[0].split(":", -1)[1]
        values = v[1]
        p.add( *jbytes( column_family, column_qualifer, values ) )
    end
table.put( p )
end

# 2. Paste and Call your put_many function with the following parameters (insert your own text for the parameters in blue):

put_many  'wiki', 'NoSQL', {
"text:" => "What JavaScript framework do you prefer?",
"revision:author" => "Eunsik Na",
"revision:comment" => "I use Vue.js" }

get 'wiki', 'NoSQL'

# Do not remove the exit call below
exit
