
local server_error = {}

local error_id = {
	ok = 0,
	
	match_faild = 1,
	match_timeout = 2,
	match_already_matching = 3,
	match_not_in_match_pool = 4,

	room_lock_in_room = 101,
	room_alloc_faild = 102,
	room_lock_faild = 103,
}

server_error.id = error_id

return server_error
