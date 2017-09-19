local skynet = require "skynet"
local server_def = require "server_def"
local sprotoloader = require "sprotoloader"

skynet.start(function()
	
	sprotoloader.register("./demo/sproto/Netproto.sproto",1);
	
     --fist service
    local gate = skynet.newservice("new_gated")
    skynet.call(gate,"lua","on_self_start_finished")

    --others services
	local login = skynet.newservice("new_logind")
	skynet.call(gate,"lua","regist_proxy",server_def.service_type.login,login)

    --last todo...
    skynet.call(gate,"lua","on_server_start_finished")
    local port = skynet.getenv("gate_port") or 8888
    max_client = skynet.getenv("gate_max_client") or 64
    print("port:"..port.." max_client:"..max_client)
	skynet.call(gate, "lua", "open" , {
		port = port,
		maxclient = tonumber(max_client),
		servername = "gated",
		nodelay = true,
	})
end)