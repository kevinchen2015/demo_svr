local skynet = require "skynet"
local skynet_manager = require "skynet.manager"
local debug_trace = require "debug_trace"
local znode = require "znode"
local JSON = require "JSON"

---base on zookkeeper node client 

local state_def = {
	ZOO_EXPIRED_SESSION_STATE = -112,
	ZOO_AUTH_FAILED_STATE = -113,
	ZOO_CONNECTING_STATE = 1,
	ZOO_ASSOCIATING_STATE = 2,
	ZOO_CONNECTED_STATE = 3,
	ZOO_NOTCONNECTED_STATE_DEF = 999,
}

local event_type_def = {
	ZOO_CREATED_EVENT = 1,
	ZOO_DELETED_EVENT = 2,
	ZOO_CHANGED_EVENT = 3,
	ZOO_CHILD_EVENT = 4,
	ZOO_SESSION_EVENT = -1,
	ZOO_NOTWATCHING_EVENT = -2,
}

local log_level_def = {
	ZOO_LOG_LEVEL_ERROR=1,
	ZOO_LOG_LEVEL_WARN=2,
	ZOO_LOG_LEVEL_INFO=3,
	ZOO_LOG_LEVEL_DEBUG=4,
}

local error_def = {
	ZOK = 0, 

	ZSYSTEMERROR = -1,
	ZRUNTIMEINCONSISTENCY = -2,
	ZDATAINCONSISTENCY = -3,
	ZCONNECTIONLOSS = -4, 
	ZMARSHALLINGERROR = -5, 
	ZUNIMPLEMENTED = -6, 
	ZOPERATIONTIMEOUT = -7, 
	ZBADARGUMENTS = -8, 
	ZINVALIDSTATE = -9, 

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

local znode_mode_def = {
	ZOO_PERSISTENT  = 0,
	ZOO_EPHEMERAL = 1,
}

------------------------------------------------------------------------

local znode_high_event = {
	EVENT_CREATE = 0,
	EVENT_DELETE = 1,
	EVENT_MODIFY = 2,
}

------------------------------------------------------------------------
local znoded_err = {
	OK = 0,
	UNKNOW,
	DISCONNECT,
	PARAM_ERROR,
}

------------------------------------------------------------------------

local CMD = {}
local zgate
local zclient_mgr
local id2peer = {}
local id2config = {}
local service2id = {}
local peer2id = {}
local selfconfig

local service_id2addr = {}

local callback_handle = {}
local interface = {}



local function connect(id,ip,port)
	print("connect to ,id:"..id.." ip:"..ip.." port:"..port)
	return skynet.call(zclient_mgr,"lua","connect",id,ip,port)
end

local function disconnect(id)
	print("disconnect,id:"..id)
	skynet.call(zclient_mgr,"lua","disconnect",id)
end


local function do_create_event(path,version,value)
	local t = JSON:decode(value)
	t.version = version
	t.conn_state = false
	id2config[t.id] = t

	if t.service then
		for _,k in ipairs(t.service) do
			assert(service2id[k]==nil)
			service2id[k] = t.id 
		end
	end
	callback_handle.on_create(t)
end

local function do_modify_event(path,version,value)
	local t = JSON:decode(value)
	local old = id2config[t.id]
	t.version = version
	t.conn_state = old.conn_state
	id2config[t.id] = t
	callback_handle.on_modify(old,t)
end

local function do_delete_event(path,version,value)
	local t = JSON:decode(value)
	disconnect(t.id)
	t = id2config[t.id]
	if t.peer then
		peer2id[t.peer] = nil
	end

	if t.service then
		for _,k in ipairs(t.service) do
			service2id[k] = nil
		end
	end
	id2config[t.id] = nil
	callback_handle.on_delete(t)
end

function znode_data_event_callback(event,path,version,value)
	print("data_event:"..event..",path:"..path..",version:"..version..",value_len:"..value:len())
	print("value:"..value)

	if event == znode_high_event.EVENT_CREATE then
		skynet.fork(do_create_event,path,version,value)
	elseif event == znode_high_event.EVENT_MODIFY then
		skynet.fork(do_modify_event,path,version,value)
	elseif event == znode_high_event.EVENT_DELETE then
		skynet.fork(do_delete_event,path,version,value)
	end
end

function znode_async_error_callback(path,op_type,result)
	print("async_error path:"..path..",op_type:"..op_type..",result:"..result)
	
end

function znode_update()
	while true do
		znode.znode_update()
		skynet.sleep(4)
	end
end

function memory_trace()
	while true do
		znode.smem_debug_print()
		skynet.sleep(2000)
	end
end

--------------------------------------------------------------------------

function CMD.init(source,config_path)
	znode.znode_set_debug_level(2)
	
	--znode.smem_debug_enable(1)
	--skynet.fork(memory_trace)

	local file = io.open(config_path,"r")
	assert(file)
	local data = file:read("*a")
	file:close()

	local config = JSON:decode(data)
	selfconfig = config

	local host = config.znode_host --"192.168.10.53:2181,192.168.10.53:2182,192.168.10.53:2183"
	local ret = znode.znode_init(host,tonumber(config.znode_timeout))    --10000
	if ret ~= 0 then 
		skynet.error("znode init error!!!!")
		return
	end

	znode.znode_set_data_event_cb(znode_data_event_callback)
	znode.znode_set_async_error_cb(znode_async_error_callback)
	skynet.fork(znode_update)

	--client mgr
	zclient_mgr = skynet.uniqueservice("zclient_mgr")
	skynet.call(zclient_mgr, "lua", "init",skynet.self())

	--set watch and get config
	for i,v in ipairs(config.zwatch_group) do
		znode.znode_add_watch_path(v,1)
		znode.znode_aget_children(v)
	end

	--svr
	zgate = skynet.uniqueservice("zgated")
	skynet.call(zgate, "lua", "open" , {
		port = tonumber(config.port),
		maxclient = tonumber(config.zgate_maxclient),
		servername = "zgated",
		nodelay = true,
	})
	skynet.call(zgate, "lua", "init",skynet.self())

	--create self node
	--local ret = znode.znode_exists(config.zname)
	--if ret ~= error_def.ZNONODE or ret == error_def.ZOK then
	--	print("exists ret:"..ret)
		znode.znode_delete(config.zname,-1)
	--end

	ret = znode.znode_create(config.zname,data,znode_mode_def.ZOO_EPHEMERAL)
	--assert(ret == error_def.ZOK)

	callback_handle.on_init(ret)
end


local function on_recv_peer_msg(peer,msg)
	callback_handle.on_recv(peer,msg)
end
--------------------------------------------------------------------
function CMD.on_recv_by_gate(source,peer,msg)
	on_recv_peer_msg(peer,msg)
end

function CMD.on_gate_agent_connected(source,peer)
	callback_handle.on_agent_connected(peer)
end

function CMD.on_gate_agent_disconnected(source,peer)
	callback_handle.on_agent_disconnected(peer)
end

--------------------------------------------------------------------
function CMD.on_recv_by_client(source,peer,msg)
	on_recv_peer_msg(peer,msg)
end

function CMD.on_client_state_changed(source,id,peer,conn_state)
	local old = id2config[id].conn_state
	id2config[id].conn_state = conn_state
	callback_handle.on_client_state_changed(id2config[id],old)
end
-------------------------------------------------------------------
function CMD.send_to(source,peer,msg)
	return interface.send_to(peer,msg)
end

function CMD.send_to_by_id(source,id,msg)
	return interface.send_to_by_id(id,msg)
end

--function CMD.send_to_group(source,group,msg)
--end

function CMD.router_to(source,msg,des_type,des_id,uid)
	return interface.router_to(msg,des_type,des_id,uid)
end

function CMD.send_to_by_group(source,group,msg)
	interface.send_to_by_group(group,msg)
end

function CMD.query_self_config(source)
	return interface.query_self_config()
end

function CMD.query_config_by_id(source,id)
	return interface.query_config_by_id(id)
end

function CMD.ping(source,...)
	return ...
end

function CMD.regist_service_addr(source,service_id,addr)
	interface.regist_service_addr(service_id,addr)
end

function CMD.query_service_addr(service_id)
	interface.query_service_addr(service_id)
end

function CMD.cmd(source,cmd,...)
	callback_handle.on_cmd(source,cmd...)
end

--------------------------------------------------------------------

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = (CMD[command])
		if f == nil then
			print("command is null:"..command)
			return 
		end
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	--skynet.exit()
end)

--------------------------------------------------------------------

function interface.query_config_by_id(id)
	return id2config[id]
end

function interface.query_self_config()
	return selfconfig
end

function interface.send_to(peer,msg)
	local id = peer2id[peer]
	if id == nil then
		return znoded_err.PARAM_ERROR
	end
	if id2config[id].conn_state then
		skynet.send(peer,"lua","send",msg)
		return znoded_err.OK
	end
	return znoded_err.DISCONNECT
end

function interface.send_to_by_id(id,msg)
	local peer = id2peer[id]
	if peer ~= nil then
		if id2config[id].conn_state then
			skynet.send(peer,"lua","send",msg)
			return znoded_err.OK
		end
	end
	return znoded_err.DISCONNECT
end

function interface.send_to_by_group(group,msg)
	for k,v in pairs(id2config[id]) do
		if v.group == group then
			interface.send_to_by_id(id,msg)
		end
	end
end

function interface.router_to(msg,des_type,des_id,src_type,src_id,uid)
	if des_id == 0 or des_id == nil then
		local id = service2id[des_type]
		if id and id2config[id] and id2config[id].peer then
			return interface.send_to(id2config[id].peer,msg)
		end
	else
		return interface.send_to_by_id(des_id,msg)
	end
	return znoded_err.UNKNOW
end

function interface.bind_peer_id(peer,id)
	peer2id[peer] = id
end

function interface.set_callback_handle(handle)
	callback_handle = handle
end

function interface.connect(id,ip,port)
	return connect(id,ip,port)
end

function interface.regist_service_addr(service_id,addr)
	service_id2addr[service_id] = addr
end

function interface.query_service_addr(service_id)
	return service_id2addr[service_id]
end

return interface