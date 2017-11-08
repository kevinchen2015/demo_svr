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

local znode_high_event = {
	EVENT_CREATE = 0,
	EVENT_DELETE = 1,
	EVENT_MODIFY = 2,
}

------------------------------------------------------------------------

local CMD = {}
local zgate
local zclient_mgr
local id2peer = {}
local id2config = {}
local selfconfig
local event_list = {}


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
	id2config[t.id] = t
	if t.id ~= selfconfig.id then
		local peer = connect(t.id,t.ip,tonumber(t.port))
		t.peer = peer
	end
end

local function do_modify_event(path,version,value)
	local t = JSON:decode(value)

end

local function do_delete_event(path,version,value)
	local t = JSON:decode(value)
	disconnect(t.id)
	id2config[t.id] = nil
end

function znode_data_event_callback(event,path,version,value)
	print("\r\ndata_event:"..event..",path:"..path..",version:"..version..",value_len:"..value:len())
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
	print("\r\nasync_error path:"..path..",op_type:"..op_type..",result:"..result)
	
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
	print("create ret:"..ret)
	--assert(ret == error_def.ZOK)
	
end


local function on_recv_peer_msg(peer,msg)
	--todo 内部协议优先解析 
end

function CMD.on_recv_by_gate(source,peer,msg)
	on_recv_peer_msg(peer,msg)
end

function CMD.on_recv_by_client(source,peer,msg)
	on_recv_peer_msg(peer,msg)
end

function CMD.send_to(source,peer,msg)
	skynet.send(peer,"lua","send",msg)
end

function CMD.send_to_by_id(source,id,msg)
	local peer = id2peer[id]
	if peer ~= nil then
		skynet.send(peer,"lua","send",msg)
	end
end

----------------------------------------------------------

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	--skynet.exit()
end)





