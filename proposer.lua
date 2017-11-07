local core = require "std.core"
local socket = require "std.socket"
local console = require "std.console"
local format = string.format

local serverid = tonumber(core.envget("serverid"))
local server_ip = {}
local server_fd = {}

local highest_num = 0

for i = 1, 15 do
	local ip = core.envget("port" .. i)
	if not ip then
		break
	end
	server_ip[i] = ip
end


local function request(req, out)
	local server_count = #server_ip
	for i = 1, server_count do
		local fd = server_fd[i]
		if not fd then
			fd = socket.connect(server_ip[i])
			server_fd[i] = fd
		end
		if fd then
			socket.write(fd, req)
		end
	end
	for i = 1, server_count do
		local fd = server_fd[i]
		if fd then
			local l = socket.readline(fd)
			if l then
				out[i] = l
			else
				server_fd[i] = nil
			end
		end
	end
end

local function prepare()
	local ret = {}
	local accept_count = 0
	local server_count = #server_ip
	local n = highest_num + 1
	local recv_num = n
	local req = format("r_prepare:%s:%s\n", n, 0)
	print("RPEPARE :", req)
	request(req, ret)
	for i = 1, server_count do
		local l = ret[i]
		print("PREPARE server:", i, "recv:", l)
		if l then
			local ack, status, val = l:match("([^:]+):([^:]+):(%d+)")
			assert(ack == "a_prepare", ack)
			if status == "accept" then
				accept_count = accept_count + 1
			elseif status == "reject" then
				val = tonumber(val)
				if recv_num < val then
					recv_num = val
				end
			else
				assert(status)
			end
		end
	end
	local half_count = server_count // 2
	print("PREPARE accept_count:", accept_count, "half_count:", half_count, "highest_num:", recv_num)
	highest_num = recv_num
	if accept_count > half_count then
		return true
	else
		--TODO:recover the value via
		--the ret from 'acceptor'
		--when prepare is rejected
		return false
	end
end

local function accept(val)
	local ret = {}
	local accept_count = 0
	local server_count = #server_ip
	local num = highest_num
	local recv_num = num
	local req = format("r_accept:%s:%s\n", num, val)
	request(req, ret)
	for i = 1, server_count do
		local l = ret[i]
		print("ACCEPT server:", i, "recv:", l)
		if l then
			local ack, status, val = l:match("([^:]+):([^:]+):(%d+)")
			assert(ack == "a_accept")
			val = tonumber(val)
			assert(val == num)
			if status == "accept" then
				accept_count = accept_count + 1
			elseif status == "reject" then
				if val > recv_num then
					recv_num = val
				end
			else
				assert(status)
			end
		end
	end
	local half_count = server_count // 2
	print("ACCEPT accept_count:", accept_count, "half_count:", half_count, "highest_num:", recv_num)
	if accept_count > half_count then
		return true
	else
		highest_num = recv_num
		return false
	end
end

console {
	addr = format("@%s", 2300 + serverid),
	cmd = {
		set = function(val, sleep)
			sleep = sleep or 0
			print("set", val, sleep)
			while true do
				while true do
					local ok = prepare()
					if ok then
						break
					end
				end
				core.sleep(sleep)
				local ok = accept(val)
				if ok then
					break
				end
			end
			return "OK"
		end,
	}
}

