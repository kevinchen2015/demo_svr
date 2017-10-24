local skynet = require "skynet"
local skynet_manager = require "skynet.manager"
local debug_trace = require "debug_trace"
local znode = require "znode"



state_def = {
	ZOO_EXPIRED_SESSION_STATE = -112,
	ZOO_AUTH_FAILED_STATE = -113,
	ZOO_CONNECTING_STATE = 1,
	ZOO_ASSOCIATING_STATE = 2,
	ZOO_CONNECTED_STATE = 3,
	ZOO_NOTCONNECTED_STATE_DEF = 999,
}

event_type_def = {
	ZOO_CREATED_EVENT = 1,
	ZOO_DELETED_EVENT = 2,
	ZOO_CHANGED_EVENT = 3,
	ZOO_CHILD_EVENT = 4,
	ZOO_SESSION_EVENT = -1,
	ZOO_NOTWATCHING_EVENT = -2,
}

log_level_def = {
	ZOO_LOG_LEVEL_ERROR=1,
	ZOO_LOG_LEVEL_WARN=2,
	ZOO_LOG_LEVEL_INFO=3,
	ZOO_LOG_LEVEL_DEBUG=4,
}

error_def = {
	ZOK = 0, 
	ZAPIERROR = -100,
	ZNONODE = -101, 
	ZNOAUTH = -102,
	ZBADVERSION = -103,
	ZNOCHILDRENFOREPHEMERALS = -108,
	ZNODEEXISTS = -110, 
	ZNOTEMPTY = -111,
	ZSESSIONEXPIRED = -112,
	ZINVALIDCALLBACK = -113,
	ZINVALIDACL = -114, 
	ZAUTHFAILED = -115,
	ZCLOSING = -116,
	ZNOTHING = -117, 
	ZSESSIONMOVED = -118,
}

znode_mode_def = {
	ZOO_PERSISTENT  = 0,
	ZOO_EPHEMERAL = 1,
}

-- for asyn_data
znode_op_type_def = {
	ZNODE_OP_CREATE = 1,
	ZNODE_OP_DELETE = 2,
	ZNODE_OP_SET = 3,
	ZNODE_OP_GET = 4,
	ZNODE_EXISTS = 5,
	ZNODE_GET_CHILDREN = 6,
}

local znode_completion_cb_def = {
	ZNODE_CB_VOID_COMPLETION = 1,
	ZNODE_CB_STAT_COMPLETION = 2,
	ZNODE_CB_DATA_COMPLETION = 3,
	ZNODE_CB_STRING_COMPLETION = 4,
	ZNODE_CB_STRINGS_COMPLETION = 5,
	ZNODE_CB_STRINGS_STAT_COMPLETION = 6,
}

-----------------------------------------------------------------------------

local node_handle = nil

local event_handle = {}
local znode_interface = {}

function znode_update()
	while true do
		if node_handle ~= nil then
			znode.znode_update(node_handle)
		end
		skynet.sleep(10)
	end
end

function znode_watch_callback(node,type,stat,path)
	print("======== type:"..type.." stat:"..stat)
	--if path:len() > 0 then
		--local ret,value = znode.znode_get(node,path)
		--print("path:"..path)
		--print("ret:"..ret)
		--print("value:"..value)

		--if event_type_def.ZOO_CHILD_EVENT == type then
			--local ret,version,t = znode.znode_get_children(node,path)
			--print("ret:"..ret)
			--print("version:"..version)
			--print(t)
			--for i,v in ipairs(t) do
			--	print(i.."=>"..v)
			--end
		--end
	--end
	event_handle.on_watch_event(node,type,stat,path)
end


function znode_void_completion(node,session,op_type,path,rc)
	print("znode_void_completion session:"..session.." op_type:"..op_type.." rc:"..rc)
end

function znode_stat_completion(node,session,op_type,path,rc,version)
	print("znode_stat_completion session:"..session.." op_type:"..op_type.." rc:"..rc)
end

function znode_data_completion(node,session,op_type,path,rc,value,version)
	print("znode_data_completion session:"..session.." op_type:"..op_type.." rc:"..rc)
	event_handle.on_get_data_rsp(node,session,path,rc,version,value)
end

function znode_string_completion(node,session,op_type,path,rc,value)
	print("znode_string_completion session:"..session.." op_type:"..op_type.." rc:"..rc)
end

function znode_strings_completion(node,session,op_type,path,rc,strings)
	print("znode_strings_completion session:"..session.." op_type:"..op_type.." rc:"..rc)
end

function znode_strings_stat_completion(node,session,op_type,path,rc,version,strings)
	print("znode_strings_stat_completion session:"..session.." op_type:"..op_type.." rc:"..rc.." path:"..path)
	print("rc:"..rc)
	print("version:"..version)
	for i,v in ipairs(strings) do
		print(i.."=>"..v)
	end
	if op_type == znode_op_type_def.ZNODE_GET_CHILDREN then
		event_handle.on_get_children_rsp(node,session,path,rc,version,strings)
	end
end


-------------------------------------------------------------------------------------------------
local cmd = {}
local config = {}

function znode_interface.init(conf,event_handle_cb)

	print("znode init ...")
	local ret = -1
	config = conf
	event_handle = event_handle_cb

	--[[
		local config = {
			debug_level = 4,
			host = "192.168.10.53:2181,192.168.10.53:2182,192.168.10.53:2183",

			watch_node_list = {
				{"/dp_room",1},
				{"/dp_hall",1},
			}

			local_node = {
				path = "/dp_hall/h1",
				addr = "127.0.0.1:10241",
			}
		}
	]]--

	znode.znode_set_debug_level(config.debug_level or 1)
	skynet.fork(znode_update)

	local host = config.host or "192.168.10.53:2181,192.168.10.53:2182,192.168.10.53:2183"
	node_handle = znode.znode_init(host,(config.timeout or 10000),znode_watch_callback)
	if node_handle == nil then
		print("node_handle can not create!!")
		return ret,nil
	end

	znode.znodeext_set_asyn_completion_callback(node_handle,znode_completion_cb_def.ZNODE_CB_VOID_COMPLETION,znode_void_completion);
	znode.znodeext_set_asyn_completion_callback(node_handle,znode_completion_cb_def.ZNODE_CB_STAT_COMPLETION,znode_stat_completion);
	znode.znodeext_set_asyn_completion_callback(node_handle,znode_completion_cb_def.ZNODE_CB_DATA_COMPLETION,znode_data_completion);
	znode.znodeext_set_asyn_completion_callback(node_handle,znode_completion_cb_def.ZNODE_CB_STRING_COMPLETION,znode_string_completion);
	znode.znodeext_set_asyn_completion_callback(node_handle,znode_completion_cb_def.ZNODE_CB_STRINGS_COMPLETION,znode_strings_completion);
	znode.znodeext_set_asyn_completion_callback(node_handle,znode_completion_cb_def.ZNODE_CB_STRINGS_STAT_COMPLETION,znode_strings_stat_completion);

	print("znode init finished!")
	
	ret = 0
	return ret,node_handle
end

function znode_interface.start()
	local ret = -1
	if config.local_node then
		ret = znode.znode_delete(node_handle,config.local_node.path,-1)
		ret = znode.znode_create(node_handle,config.local_node.path,config.local_node.addr,znode_mode_def.ZOO_EPHEMERAL)
	end

	if ret ~= error_def.ZOK then
		print("create node ret:"..ret)
	end
	if config.watch_node_list then
		for i,v in ipairs(config.watch_node_list) do
			print("znode watch path:"..v[1].." has_child:",v[2])
			znode.znodeext_add_watch_path(node_handle,v[1],v[2])
		end
	else
		print("config.watch_node_list is nil")
	end
	return ret
end

function znode_interface.exit()
	if config.local_node then
		--sync api
		znode.znode_delete(node_handle,config.local_node.path,-1)
	end
end

function znode_interface.get_znode_handle()
	return node_handle
end

function znode_interface.znode_aget(handle,path,session)
	znode.znode_aget(node_handle,path,session)
end

function znode_interface.znode_aget_children(handle,path,session)
	znode.znode_aget_children(handle,path,session)
end

return znode_interface


