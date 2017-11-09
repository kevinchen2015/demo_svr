local skynet = require "skynet"
local znode_interface = require "znoded"

znoded_calllback_handle = {}

------游戏业务逻辑层

local bnode_state = {
	ready 	= 1,
	active 	= 2,
	offline = 3,
	deactive = 4,
}


local cmd_handler = {}

local function check_ready(t)
	print(t.state)
	if tonumber(t.state) == bnode_state.ready then
		local peer = znode_interface.connect(t.id,t.ip,tonumber(t.port))
		t.peer = peer
		znode_interface.bind_peer_id(t.peer,t.id)
	end
end


function znoded_calllback_handle.on_init(result)
	print("create result:"..result)

end

function znoded_calllback_handle.on_create(config)
	if config.id ~= znode_interface.query_self_config().id then
		check_ready(config)
	end
end

function znoded_calllback_handle.on_modify(old_config,new_config)
	if old_config.state ~= new_config.state then
		check_ready(new_config)
	end
end

function znoded_calllback_handle.on_delete(config)
	
end

function znoded_calllback_handle.on_recv(peer,msg)
	--parse msg
	print("on recv msg:"..msg)
end

function znoded_calllback_handle.on_agent_connected(peer)
	
end

function znoded_calllback_handle.on_agent_disconnected(peer)
	
end

local function test_send(id)
	local config = true
	while config do
		config = znode_interface.query_config_by_id(id)
		if config == nil or config.conn_state == false then
			break
		end
		znode_interface.send_to_by_id(id,"hello!!!! i am "..config.id)
		skynet.sleep(4)
	end
end

function znoded_calllback_handle.on_client_state_changed(config,old_state)
	if config.conn_state then
		skynet.fork(test_send,config.id)
	end
end

function znoded_calllback_handle.on_cmd(source,cmd,...)
	cmd_handler[cmd](source,...)
end

--------------------------------------------------------------------


znode_interface.set_callback_handle(znoded_calllback_handle)