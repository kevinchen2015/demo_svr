package.cpath = "luaclib/?.so"
package.path = "lualib/?.lua;examples/?.lua"

local socket = require "client.socket"
local skynet = require "skynet"

local client = {}
local client_mt = {__index = client}

function client.create_client(recv_cb)
	local new_client = {
		fd=nil,
		last="",
		ip="",
		port=0,
		recv_cb = recv_cb
	}
	return setmetatable(new_client, client_mt)
end

function client:connect(ip,port)
	self:close()
	self.ip = ip
	self.port = port
	self.fd = socket.connect(ip, port)

	if self.fd ~= nil then
		skynet.timeout(1000,function()
			if self:is_connect() == false then
				return
			end
			print("heat check!")
		end)
		skynet.fork(self.dispatch_package_text,self,self.recv_cb)
	end
	return self.fd
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
		socket.closed(self.fd)
		self.fd = nil
		self.last = ""

		print("client:close()")
	end
end

function client:send(text)
	if self.fd == nil then
		self:reconnect()
		if self.fd == nil then
			return
		end
	end
	local package = string.pack(">s2", text)
	socket.send(self.fd, package)
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
	return text:sub(3,2+s), text:sub(3+s)
end

local function recv_package(fd,last)
	local result
	result, last = unpack_package(last)
	if result then
		return result, last
	end
	local r = socket.recv(fd)
	if not r then
		return nil, last
	end
	if r == "" then
		--error "Server closed"
		return nil,"",1
	end
	return unpack_package(last .. r)
end

function client:dispatch_package_text(recv_cb)
	while true do
		local v
		v, last,error = recv_package(self.fd,self.last)
		if error ~= nil then
			self:close()
			local fn = function()
				if self:is_connect() then
					return
				end
				self:reconnect()

				if self.fd == nil then
					skynet.timout(500,fn)
				end
			end
			skynet.timout(500,fn)
			break
		end

		if not v then
			break
		end
		--print("recv:"..v)
		--todo dispatch msg
		recv_cb(self,v)
	end
end

return client