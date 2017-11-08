local gateserver = require "snax.gateserver"
local skynet = require "skynet"
local driver = require "skynet.socketdriver"

local gate_cmd = {}
local server_handle = {}
local fd2agent={}
local temp_redis_service 
local free_db_agentd_pool = {}
local receiver_service

local agent_name = "zagentd"

--------------------------------------------------------------

function server_handle.connect(fd,msg)
	print("connect:"..fd.."|"..msg)
	gateserver.openclient(fd)
	local agent = nil
	if #free_db_agentd_pool > 0 then
		agent = free_db_agentd_pool[1]
		table.remove(free_db_agentd_pool,1)
	else
	  	agent = skynet.newservice(agent_name)
	end
	fd2agent[fd] = agent
	skynet.call(agent,"lua","on_connect",fd,skynet.self())
	return agent
end

function server_handle.disconnect(fd)
	print("disconnect:"..fd)
	local agent = fd2agent[fd]
	skynet.call(agent,"lua","on_disconnect")
	table.insert(free_db_agentd_pool,agent)
	fd2agent[fd] = nil
end

function server_handle.message(fd,msg,sz)
	print("message:"..fd.." msg size:"..sz)
	local agent = fd2agent[fd]
	local text = skynet.tostring(msg,sz)
	skynet.send(receiver_service,"lua","on_recv_by_gate",agent,text)
end

function server_handle.error(fd,msg)
	print("error:"..fd.."|"..msg)
	gateserver.closeclient(fd)
end

function server_handle.warning(fd,size)
	print("warning:"..fd.."|"..size)
end

function server_handle.command(cmd,address,...)
	gate_cmd[cmd](...)
end

function gate_cmd.init(receiver)
	receiver_service = receiver
end
--------------------------------------------------------------------

skynet.register_protocol({
	name = "client",
	id = skynet.PTYPE_CLIENT,
})

gateserver.start(server_handle)

skynet.init(function()


end,"init")