
local room_def = {}

local room_lock_state = {
	idle = 0,
	locking = 1,
	locked = 2,
}

room_def.room_lock_state = room_lock_state
room_def.room_group_service_name = "room_group"
room_def.room_manager_service_name = "room_manager"
room_def.room_route_service_name = "room_route"
room_def.room_service_prefix = "room_"


return room_def
