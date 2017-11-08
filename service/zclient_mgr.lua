local skynet = require "skynet"

local free_client_pool = {}
local CMD = {}
local id2client = {}

local receiver_service

local zclient_name = "zclient"  -- todo custom

local function create_zclient()
	local client = nil
	if #free_client_pool > 0 then
		client = free_client_pool[1]
		table.remove(free_client_pool,1)
	else
		client = skynet.newservice(zclient_name)
		skynet.call(client,"lua","init",receiver_service)
	end
	return client
end

local function release_zclient(client)
	skynet.call(client,"lua","disconnect")
	table.insert(free_client_pool,client)
end

-----------------------------------------------------------

function CMD.init(source,receiver)
	receiver_service = receiver
end

function CMD.connect(source,id,ip,port)
	local zclient = create_zclient()
	id2client[id] = zclient
	skynet.send(zclient,"lua","connect",ip,port)
	return zclient
end

function CMD.disconnect(source,id)
	if id2client[id] ~= nil then
		local c = id2client[id]
		id2client[id] = nil
		release_zclient(c)
	end
end

function CMD.query_client(source,id)
	return id2client[id]
end

function CMD.send_to(source,id,msg)
	if id2client[id] ~= nil then
		skynet.send(client,"lua","send",msg)
	end
end

skynet.start(function()
	
	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)

	--skynet.exit()
end)
