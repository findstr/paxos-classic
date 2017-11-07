local core = require "std.core"
local socket = require "std.socket"
local format = string.format

local serverid = core.envget("serverid")
local port = core.envget("port" .. serverid)

local highest_num = 0
local value = nil

socket.listen(port, function(fd, addr)
	print("accept", fd, addr)
	while true do
		local l = socket.readline(fd)
		if not l then
			return
		end
		local req, num, val = l:match("([^:]+):(%d+):([^:]+)")
		assert(req, l);
		num = tonumber(num)
		if req == "r_prepare" then
			local ack
			local accept
			if num > highest_num then
				highest_num = num
				accept = "accept"
			else
				accept = "reject"
			end
			ack = format("a_prepare:%s:%s\n", accept, highest_num)
			socket.write(fd, ack)
			print(l, ack)
		elseif req == "r_accept" then
			local ack
			local accept
			local retv
			if highest_num > num then
				accept = "reject"
				retv = highest_num
			else
				value = val
				retv = highest_num
				accept = "accept"
			end
			ack = format("a_accept:%s:%s\n", accept, retv)
			socket.write(fd, ack)
			print(l, ack)
		end
	end
end)

