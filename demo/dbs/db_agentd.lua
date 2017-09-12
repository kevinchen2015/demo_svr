local skynet = require "skynet"
local socket = require ("skynet.socket")

local arg = table.pack(...)
local fd = tonumber(arg[1])
local gate = tonumber(arg[2])

local CMD = {}

function CMD.send(source,msg)
	local package = string.pack(">s2", msg)
	socket.write(fd, package)
end


skynet.register_protocol{
	name = "client",
	id = skynet.PTYPE_CLIENT,
	unpack = skynet.tostring,
}


skynet.start(function()
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	--todo time heart and login check
	print("new agnet:"..skynet.self())

end)
