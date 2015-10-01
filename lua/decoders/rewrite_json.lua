require "cjson"
local json_helpers = require "json_helpers"

function process_message()
  local pok, json = pcall(cjson.decode, read_message("Payload"))
  if not pok then return -1, "Failed to decode JSON." end


  1. Get move_fields setting.
  2. Add keep setting as entries in move_fields setting
  3. Add support for remove setting (list of dotted field names)
  4. Go through all fields from AF rails log and configure any rewrites
  5. Add config option to provide an expression to transform values (to change microseconds to milliseconds)

  write_message("Payload", cjson.encode(json))
  return 0
end
