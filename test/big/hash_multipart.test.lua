dofile('utils.lua')

hash = box.schema.create_space('tweedledum')
hash:create_index('primary', 'hash', {parts = {0, 'num', 1, 'str', 2, 'num'}, unique = true })
hash:create_index('unique', 'hash', {parts = {2, 'num', 4, 'num'}, unique = true })

-- insert rows
hash:insert(0, 'foo', 0, '', 1)
hash:insert(0, 'foo', 1, '', 1)
hash:insert(1, 'foo', 0, '', 2)
hash:insert(1, 'foo', 1, '', 2)
hash:insert(0, 'bar', 0, '', 3)
hash:insert(0, 'bar', 1, '', 3)
hash:insert(1, 'bar', 0, '', 4)
hash:insert(1, 'bar', 1, '', 4)

-- try to insert a row with a duplicate key
hash:insert(1, 'bar', 1, '', 5)

-- output all rows

--# setopt delimiter ';'
function box.select_all(space)
    local result = {}
    for k, v in hash:pairs() do
        table.insert(result, v)
    end
    return result
end;
--# setopt delimiter ''
box.sort(box.select_all(0))

-- primary index select
hash.index['primary']:select({1, 'foo', 0})
hash.index['primary']:select({1, 'bar', 0})
-- primary index select with missing part
hash.index['primary']:select({1, 'foo'})
-- primary index select with extra part
hash.index['primary']:select({1, 'foo', 0, 0})
-- primary index select with wrong type
hash.index['primary']:select({1, 'foo', 'baz'})

-- secondary index select
hash.index['unique']:select({1, 4})
-- secondary index select with no such key
hash.index['unique']:select({1, 5})
-- secondary index select with missing part
hash.index['unique']:select({1})
-- secondary index select with wrong type
hash.index['unique']:select({1, 'baz'})

-- cleanup
hash:truncate()
hash:len()
hash:drop()
