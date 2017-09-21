local skynet = require "skynet"
local room_def = require "room_def"
local debug_trace = require "debug_trace"
local node_util = require "static_node_util"
local center_route_util = require "center_route_util"
local server_def = require "server_def"
local server_error = require "server_error"

local command = {}

--对room group 和 room的管理，目前挂在中心服节点上

local group_node = {}
local room_group_info = {} 
local user_room_info = {}

-------------------------------------------------------------------

function command.query_user_room_info(source,uid)
	return user_room_info[uid]
end

function command.add_group(source,node_name,group_info)
	group_node[group_info.id] = {name = node_name,addr=source}
	room_group_info[group_info.id] = group_info.rooms
	print("room_managerd.add_group node_name:"..node_name)
	--debug_trace.print_r(group_info)
end

function command.remove_group(source,node_name,group_info)
	print("room_managerd.remove_group node_name:"..node_name)
	room_group_info[group_info.id] = nil
	group_node[group_info.id] = nil
end

function command.on_recv_group_report(source,group_id,group_info)
	room_group_info[group_info.id] = group_info.rooms
end

local function notify_room_event(des_type,des_service,...)
	--通过中心服务准发notify
	local ret = center_route_util.send_to_node_by_type(des_type,des_service,...)
end

function command.lock_room(source,group_id,room_id,uid_list)
	print("room_managerd.lock_room gourp_id:"..group_id.." room_id:"..room_id)
	local group_addr = group_node[group_id].addr
	local group_name = group_node[group_id].name
	local room = room_group_info[group_id][room_id]
	room.lock_state = room_def.room_lock_state.locking
	local rsn = node_util.query_remote_server_name(group_name,room_def.room_group_service_name)
	local ret = node_util.call(group_name,rsn,"lock_room",group_id,room_id,uid_list)
	if ret == 0 then
		room.lock_state = room_def.room_lock_state.locked
		room.uid_list = uid_list
		for i,v in ipairs(uid_list) do
			user_room_info[v] = {gid=group_id,rid=room_id}
		end
		notify_room_event(server_def.node_type.gate_node,room_def.room_route_service_name,"notify_room_locked",group_id,room_id,uid_list)
		return 0
	else
		room.lock_state = room_def.room_lock_state.idle
		room.uid_list = {}
	end
	return server_error.id.room_lock_faild
end

function command.on_recv_room_unlocked(source,group_id,room_id,uid_list)
	print("room_managerd.unlock_room gourp_id:"..group_id.." room_id:"..room_id)
	local room = room_group_info[group_id][room_id]
	room.lock_state = room_def.room_lock_state.idle
	for i,v in ipairs(uid_list) do
			user_room_info[v] = nil
		end
	notify_room_event(server_def.node_type.gate_node,room_def.room_route_service_name,"notify_room_unlock",group_id,room_id,uid_list)
end

function command.lock_idle_room(source,...)
	local ret = server_error.id.room_alloc_faild
	for k,v in pairs(room_group_info) do
		for i,r in ipairs(v) do
			if r.lock_state == room_def.room_lock_state.idle then
				return command.lock_room(source,k,i,...),k,i
			end
		end
	end
	return ret
end

skynet.start(function()
	skynet.dispatch("lua", function(session, source, cmd, ...)
		local f = assert(command[cmd])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
end)
