local geteserver = require "snax.gateserver"
local crypt = require "skynet.crypt"
local skynet = require "skynet"
local pb = require "protobuf"
local driver = require "skynet.socketdriver"

local gate_cmd = {}
local server_handle = {}

local fd2agent={}

local temp_redis_service 

--------------------------------------------------------------

function server_handle.connect(fd,msg)
	print("connect:"..fd.."|"..msg)
	geteserver.openclient(fd)
	local agent = skynet.newservice("db_agentd",fd,skynet.self())
	fd2agent[fd] = agent
end

function server_handle.disconnect(fd)
	print("disconnect:"..fd)
	fd2agent[fd] = nil
end

local function send_text(fd,text)
	local package = string.pack(">s2", text)
	driver.send(fd, package)
end

function server_handle.message(fd,msg,sz)
	print("message:"..fd.." msg size:"..sz)
	local text = skynet.tostring(msg,sz)
	local agent = fd2agent[fd]
	skynet.redirect(temp_redis_service , agent, "client", 0, msg,sz)
end

function server_handle.error(fd,msg)
	print("error:"..fd.."|"..msg)
	geteserver.closeclient(fd)
end

function server_handle.warning(fd,size)
	print("warning:"..fd.."|"..size)
end

function server_handle.command(cmd,address,...)
	print("lua command:"..cmd)
	gate_cmd[cmd](...)
end


--------------------------------------------------------------------

skynet.register_protocol({
	name = "client",
	id = skynet.PTYPE_CLIENT,
})

geteserver.start(server_handle)

skynet.init(function()

	temp_redis_service = skynet.newservice("redis_cli_service")
	conf = {
		host = "127.0.0.1",
		port = 19000,
		db = 0,
	}
	skynet.send(temp_redis_service,"lua","open",conf)

end,"init")