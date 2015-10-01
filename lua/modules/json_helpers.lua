local pairs = pairs
local tostring = tostring
local type = type
local string = require "string"
local table = require "table"

local M = {}
setfenv(1, M)


function flatten_and_move(t, prefix, move_specs)
  local new_t = {}
  move_fields(flat, new_t, move_specs)
  new_t[prefix] = {}
  flatten_table(t, new_t[prefix])
  return new_t
end

-- Moves fields from a location in one table to locations in another table
function move_fields(from_t, to_t, move_specs)
  for from, to in pairs(move_specs) do
    local val = remove_sub_table(from_t, dot_split(from))
    if val then
      if not set_dotted(to_t, to, val) then
        -- If set failed for some reason, put it back where it was.
        set_dotted(from_t, from, val)
    end
  end
end

function set_dotted(t, path, val)
  local to_parts = dot_split(path)
  local last_k = table.remove(to_parts)
  local dest = t
  for _, k in pairs(to_parts) do
    if type(dest) ~= "table" then
      return false
    end
    if not dest[k] then
      dest[k] = {}
    end
    dest = dest[k]
  end
  dest[last_k] = val
  return true
end

function remove_sub_table(t, path)
  -- If it isn't a table, bail out.
  if type(t) ~= "table" then return nil end

  local key = table.remove(path, 1)
  local sub_table = t[key]
  if not sub_table then return nil end

  -- At the last key, remove and return the value.
  if not #path then
    t[key] = nil
    return sub_table
  end

  -- Go on to the next key.
  local val = remove_sub_table(sub_table, path)
  if not val then return nil end

  -- If no more keys, remove whole sub-table.
  if numkeys(sub_table) == 0 then t[key] = nil end

  return val
end

function numkeys(t)
  local len = 0
  for _ in pairs(t) do
    len = len + 1
  end
  return len
end

-- Flattens a table with an optional prefix
function flatten_table(t, flat, prefix)
  for k, v in pairs(t) do
    if prefix then
      full_key = string.format("%s.%s", prefix, k)
    else
      full_key = k
    end

    if type(v) == "table" then
      if is_array(v) then
        flat[full_key] = tostring_array(v)
      else
        flatten_table(v, flat, full_key)
      end
    else
      if type(v) != "userdata" then
        flat[full_key] = tostring(v)
      end
    end
  end
end

-- Returns a copy of an array with the values all turned into strings
function tostring_array(a)
  string_array = {}
  for _, v in pairs(a) do
    table.insert(string_array, tostring(v))
  end
  return string_array
end

function is_array(t)
    local i = 0
    for _ in pairs(t) do
        i = i + 1
        if t[i] == nil then return false end
    end
    return true
end

function dot_split(s)
  local parts = {}
  for part in string.gmatch(s, '[^\\.]+') do
    table.insert(parts, part)
  end
  return parts
end

return M
