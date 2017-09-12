
local socket = require "client.socket"
local skynet = require "skynet"

--简单的socket封装，只提供链接，发送，接收，解析gate对应的封包结构，不做心跳检测，不做重连处理
--提供在接收包协程做recv_cb + msg handle 的处理，也提供默认的处理方式：放入队列，另外的协程获取处理

local client = {}
local client_mt = {__index = client}

local function default_recv_handle(cli,msg)
	table.insert(cli.recv_msg_queue,msg)
end

function client.create_client(recv_cb)
	local new_client = {
		fd=nil,
		last="",
		ip="",
		port=0,
		recv_msg_queue = {},
		recv_cb = recv_cb or default_recv_handle,
	}
	return setmetatable(new_client, client_mt)
end

function client:read_one_msg_from_queue()
	if #self.recv_msg_queue > 0 then
		local ret = self.recv_msg_queue[1]
		table.remove(self.recv_msg_queue,1)
		return ret
	else
		return nil
	end
end


function client:connect(ip,port)
	self:close()
	self.ip = ip
	self.port = port
	local ret = socket.connect(ip, port)
	if type(ret) == "number" then
		self.fd = ret
		skynet.fork(self.dispatch_package_text,self,self.recv_cb)
	end
	return self.fd,ret
end

function client:reconnect()
	if self.port > 0 then
		print("client:reconnect()")
		self:connect(self.ip,self.port)
	end
end

function client:is_connect()
	return self.fd ~= nil
end

function client:close()
	if self.fd ~= nil then
		local fd = self.fd
		self.fd = nil
		self.last = ""
		self.recv_msg_queue = {}
		socket.close(fd)
	end
end

function client:send(text)
	if self:is_connect() then
		local package = string.pack(">s2", text)
		return socket.send(self.fd, package)
	end
	return 0
end

local function unpack_package(text)
	local size = #text
	if size < 2 then
		return nil, text
	end
	local s = text:byte(1) * 256 + text:byte(2)
	if size < s+2 then
		return nil, text
	end
	local r,l = text:sub(3,2+s), text:sub(3+s)
	return r,l
end

local function recv_package(cli)
	local result
	result, cli.last = unpack_package(cli.last)
	if result then
		return result, cli.last
	end
	local r = socket.recv(cli.fd)
	if not r then
		return nil, cli.last
	end
	if r == "" then
		--print("Server closed")
		return nil,"",1
	end
	return unpack_package(cli.last .. r)
end

function client:dispatch_package_text(recv_cb)
	while self.fd do

		skynet.sleep(0)
		local v
		v, self.last,err_id = recv_package(self)
		if v then
		print(v)
		end
		if err_id ~= nil then
			self:close()
			break
		end

		if v then
			recv_cb(self,v)
		end

	end
end

return client