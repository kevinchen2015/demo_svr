local skynet = require "skynet"
local client_factory = require "client0"

local client = nil
local ip
local id
local port
local CMD = {}
local receiver_service

local zclient_interface = {}

local last_connect_state = false

local function recv_once()
	local msg = client:read_one_msg_from_queue()
	while msg ~=nil do
		--print("recv_len:"..msg:len())
		skynet.send(receiver_service,"lua","on_recv_by_client",skynet.self(),msg)
		msg = client:read_one_msg_from_queue()
	end
end

local function reconnect()
	if client and client:is_connect() == false then
		client:reconnect()
	end
end

local function recv_handle()
	while client ~= nil do
		recv_once()
		local is_connect = client:is_connect()
		if is_connect ~= last_connect_state then
			skynet.send(receiver_service,"lua","on_client_state_changed",id,skynet.self(),last_connect_state)
			last_connect_state = is_connect
		end

		if is_connect == false then
			print("socket is disconn,reconn...")
			skynet.fork(reconnect)
			skynet.sleep(500)
		else
			--todo..heart beat
			--zclient_interface.heart_beat(skynet.self,client)
		end
		skynet.sleep(2)
	end
end

--------------------------------------------------

local function close_client()
	if client ~= nil then
		recv_once()
		client:close()
		client = nil
		last_connect_state = false
	end
end

function CMD.send(source,msg)
	if client == nil then 
		return 
	end
	client:send(msg)
end

local function connect(ip,port)
	if client and client:is_connect() == false then
		client:connect(ip,port)
	end
end

function CMD.connect(source,svr_ip,svr_port,svr_id)
	
	if client ~= nil then
		if client:is_connect() and ip == svr_ip and port == svr_port then
			return
		end
		close_client()
	end

	id = svr_id
	ip = svr_ip
	port = svr_port
	client = client_factory.create_client()

	skynet.fork(connect,ip,port)
	skynet.fork(recv_handle)
	
end

function CMD.disconnect(source)
	close_client()
end

function CMD.is_connect(source)
	if client then
		return client:is_connect() 
	end
	return false
end

function CMD.init(source,receiver)
	receiver_service = receiver
end

skynet.start(function()

	skynet.dispatch("lua", function(session, source, command, ...)
		local f = assert(CMD[command])
		skynet.ret(skynet.pack(f(source, ...)))
	end)
	--todo time heart
	print("new agnet:"..skynet.self())
	
	--skynet.exit()
end)

--maybe
function set_interface(interface)
	zclient_interface = interface
end
