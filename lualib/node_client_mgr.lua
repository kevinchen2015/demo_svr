
local skynet = require "skynet"
local client = require "client0"
local string_util = require "string_util"

local node_client_mgr = {}
local client_info_set = {}

local function node_client_mgr_update()
    while true do
        skynet.sleep(500)

        for k,v in pairs(client_info_set) do 
            if v.invaild == false then
                if v.client:is_connect() then
                    --heart beat ,todo
                else
                    --reconnect ,todo
                    print("reconnect:")
                    v.client:reconnect()
                end
            end 
        end
    end
end

function node_client_mgr.init()
    skynet.fork(node_client_mgr_update)
end

function node_client_mgr.get_ip_port(addr)
    local t = string_util.split(addr,":")
    return t[1],t[2]
end

function node_client_mgr.connect_to(path,addr,version)
    local client_info = node_client_mgr.get_client_info_by_path(path)
    if client_info == nil then
        client_info = {}
        client_info_set[path] = client_info
        client_info.client = client.create_client()
    end
    client_info.invaild = false
    client_info.addr = addr
    client_info.version = version
    if client_info.addr == addr and client_info.client:is_connect() then
        print("already connected!!!")
        return
    end
    print("connect to:"..addr.." path:"..path)
    local ip,port = node_client_mgr.get_ip_port(addr)
    --print("ip:"..ip.." port:"..port)
    client_info.client:connect(ip,port)
end

function node_client_mgr.disconnect(path)
    print("disconnect :"..path)
    local client_info = node_client_mgr.get_client_info_by_path(path)
    if client_info then
        client_info.invaild = true
        client_info.client:close()
        --del
        client_info_set[path] = nil
    end
end

function node_client_mgr.change_connect_to(path,addr,version)

    local client_info = node_client_mgr.get_client_info_by_path(path)
    if client_info == nil or client_info.invaild == true then
        node_client_mgr.connect_to(path,addr,version)
        return
    end
    if client_info.version < version then
        print("change connect to:"..addr.." path:"..path)
        client_info.version = version
        client_info.invaild = false
        local ip,port = node_client_mgr.get_ip_port(addr)
        client_info.client:connect_to(ip,port)
    else
        print("version is old")
    end
end

function node_client_mgr.get_client_by_path(path)
    local client_info = node_client_mgr.get_client_info_by_path(path)
    if client_info == nil then
        return nil
    end
    return client_info.client
end

function node_client_mgr.get_client_info_by_path(path)
    local client_info = client_info_set[path]
    return client_info
end


return node_client_mgr