local skynet = require("skynet")
local node_util = require "static_node_util"
local sprotoloader = require "sprotoloader"
local service_util = require "service_util"
local package_proto = require "package.proto"
local debug_trace = require "debug_trace"
local server_def = require "server_def"
local sproto = require "sproto"

local match_node = "center_node"

local command = {}

local proto
local proto_host

local respones_handle = {}

--function command.notify_room_locked(source,ret,uid,group_id,room_id,uid_list)
--   print("notify_match_result,ret:"..ret)
--end

function respones_handle.match_request(uid,args)
    print("match_request:"..args..ret)
end

function respones_handle.match_cancle(uid,args)
   print("match_cancle:"..args..ret)
end

function respones_handle.match_result(uid,args)
    print("match_result:"..args.ret)

    if args.ret == 0 then
        print("room_id:"..args.room_id)
    end
end

function command.on_route_message(source,msg,sz,uid)
    print("on_route_message_to_user:")

    local body,des_type,des_id,src_type,src_id = package_proto.decode(msg,sz)
    print(des_type)
    print(des_id)
    print(src_type)
    print(src_id)
    local type,name,args = proto_host:dispatch(body,body:len())
    print(type)
    print(name)
    debug_trace.print_r(args)

    respones_handle[name](uid,args)
  
    --if type(uid_list) == "table" then
        --todo sendto
    --else
        --todo sendto
    --end
end

function test_call() 
    return node_util.query_remote_server_name(match_node,"match")
end

skynet.start(function()
    
    local proto_id = 1 
	sprotoloader.register("./work/sproto/match_proto.sproto",proto_id)
	proto = sprotoloader.load(proto_id)
	proto_host = proto:host "package"

    skynet.dispatch("lua", function(session, source, cmd, ...)

		local f = command[cmd]
        if f == nil then
            print("cmd:"..cmd)
            assert(f)
        end
		skynet.ret(skynet.pack(f(source, ...)))
	end)

    node_util.register("gate",skynet.self())
    
    local room_route = skynet.uniqueservice("room_routed")
    node_util.register("room_route",room_route)

    --local match = node_util.query_remote_server_name(match_node,"match")
    local center = node_util.query_remote_server_name("center_node","center_route")
    print(center)
    --test cmd
    --node_util.call(match_node,match,"add_to_match_pool",11)
    --node_util.call(match_node,match,"add_to_match_pool",22)

    --test sproto
   
    local client = proto:host "package"
    local client_request = client:attach(proto)

    local msg ,tag = client_request("match_request",{match_type=1})
  
    --print("tag:"..tag)
    --local v = proto:request_decode("match_request",msg)
    --debug_trace.print_r(v)

    --local m1 = client_request("match_result", { ret = 0, group_id=gid,room_id=rid,uid_list = uids},1)
    --local type,name,args = proto_host:dispatch(m1,m1:len())
    --print(type)
    --debug_trace.print_r(args)

    local message = package_proto.encode(msg,msg:len(),server_def.service_type.match,0,0,0)
    --local type,name,args,respones = proto_host:dispatch(body,body:len())
    --print(type)
    --print(name)
    --debug_trace.print_r(args)
    
    --print(message)
    --local m2,m2_len,uid = node_util.call(match_node,match,"on_route_message",message,message:len(),11)
    node_util.send("center_node",center,"route_message",message,message:len(),11)
    node_util.send("center_node",center,"route_message",message,message:len(),22)

    --skynet.exit()
end)