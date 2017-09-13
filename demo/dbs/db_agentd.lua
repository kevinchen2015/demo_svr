local skynet = require "skynet"
local socket = require ("skynet.socket")


local fd 
local gate 

local CMD = {}

function CMD.send(source,msg)
	if fd == nil then 
		return 
	end

	local package = string.pack(">s2", msg)
	socket.write(fd, package)
end

function CMD.on_connect(source,socket_fd,gate_service)
	fd = socket_fd
	gate = gate_service
end

function CMD.on_disconnect(source)
	fd = nil
end

skynet.start(function()
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	--todo time heart and login check
	print("new agnet:"..skynet.self())
end)
