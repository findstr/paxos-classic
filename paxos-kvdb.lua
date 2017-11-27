local core = require "sys.core"
local socket = require "sys.socket"
local console = require "sys.console"
local json = require "sys.json"
local format = string.format

local serverid = tonumber(core.envget("serverid"))
local port = core.envget("port" .. serverid)

local server_ip = {}
local server_fd = {}

for i = 1, 15 do
	local ip = core.envget("port" .. i)
	if not ip then
		break
	end
	server_ip[i] = ip
end

--TODO OPTIMISE: don't connect self
local function request(req, out)
	req = json.encode(req) .. "\n"
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
				out[i] = assert(json.decode(l))
			else
				server_fd[i] = nil
			end
		end
	end
	return server_count
end

-----db
--for learner
local LEARN_VALUE = nil
--for proposer
local LAST_TRIED = 0
--for acceptor
local PREV_VOTE = nil
local PREV_BAL = 0

local PROPOSE_BAL = 0

-----proposer
local function prepare(v)
	local ret = {}
	local b = LAST_TRIED + 1
	local max_ballot = 0
	local max_value = nil
	local max_tried = b
	local ballot_count = {[0] = 0}
	local req = {
		cmd = "prepare",
		ballot = b,
	}
	local count = 0
	local server_count = request(req, ret)
	for i = 1, server_count do
		local ack = ret[i]
		print("ack", json.encode(ack))
		if ack and ack.ballot == b then
			local cmd = ack.cmd
			local n = ballot_count[ack.value_bal] or 0
			ballot_count[ack.value_bal] = n + 1
			if cmd == "promise" then
				count = count + 1
			else
				if ack.propose_bal > max_tried then
					max_tried = ack.value_bal
				end
			end
			if ack.value_bal > max_ballot then
				max_ballot = ack.value_bal
				max_value = ack.value
			end
		end
	end
	local max_ballot_count = ballot_count[max_ballot]
	local half_count = server_count // 2
	print("[Prepare] ballot:", b, "value:", v, "last tried:",
		max_tried, "count:", count, "half_count:", half_count,
		"max_ballot", max_ballot,
		"max_ballot_count", max_ballot_count)
	if count <= half_count then
		local ret = nil
		LAST_TRIED = max_tried
		if max_ballot_count > half_count then
			LEARN_VALUE = max_value
		end
		return nil, ret
	end
	LAST_TRIED = b
	if max_ballot_count > half_count then
		--a major of acceptor has choosen this value,
		--so we can set a new value,
		--otherwise, we must promise a major of acceptors
		--has choosen the max_value
		max_value = v
	end
	print("PREPARE OK", max_tried, max_value)
	return b, max_value
end

local function setval(v)
	local max_tried, max_value = prepare(v)
	if not max_tried then
		return format("FAIL(%s)", max_value)
	end
	--accept
	local req = {}
	req.cmd = "accept"
	req.ballot = max_tried
	req.value = max_value
	ret = {}
	count = 0
	print("max_ballot", max_tried, max_value)
	server_count = request(req, ret)
	for i = 1, server_count do
		local ack = ret[i]
		if ack and ack.ballot == max_tried then
			local cmd = ack.cmd
			if cmd == "accepted" then
				count = count + 1
			end
		end
	end
	print("accept count:", count)
	if count < (server_count // 2) then
		return format("FAIL(%s)", max_value)
	end
	--learn
	req.cmd = "learn"
	req.value = max_value
	ret = {}
	request(req, ret)
	return "Success:" .. tostring(max_value)
end

-----acceptor

socket.listen(port, function(fd, addr)
	while true do
		local l = socket.readline(fd)
		if not l then
			return
		end
		print("recv:", l)
		local req = assert(json.decode(l), l)
		local ack = {} --single a 'ack.cmd' field, means fail
		if req.cmd == "prepare" then
			-- ack{cmd, ballot, value_bal, value}
			local ballot = req.ballot
			print("promise:", ballot, PROPOSE_BAL, PREV_BAL, PREV_VOTE)
			ack.ballot = ballot
			ack.value_bal = PREV_BAL
			ack.value = PREV_VOTE
			if ballot > PROPOSE_BAL then --old instance
				PROPOSE_BAL = ballot
				ack.cmd = "promise"
			else
				ack.cmd = "promise_reject"
				ack.propose_bal = PROPOSE_BAL
			end
		elseif req.cmd == "accept" then
			--ack {cmd, ballot, value}
			if req.ballot == PROPOSE_BAL then
				ack.cmd = "accepted"
				if req.value then
					PREV_BAL = req.ballot
					PREV_VOTE = req.value
				end
				ack.ballot = req.ballot
				ack.value = req.value
			else
				ack.cmd = "accept_reject"
			end
		elseif req.cmd == "learn" then --as learner
			ack.cmd = "learned"
			LEARN_VALUE = req.value
		end
		local al = json.encode(ack) .. "\n"
		print("ack:", al)
		assert(socket.write(fd, al))
	end
end)

console {
	addr = format(":%s", 2300 + serverid),
	cmd = {
		set = function(val, sleep)
			local ret = setval(val)
			return ret
		end,
		get = function (val)
			if not LEARN_VALUE then
				prepare()
			end
			return tostring(LEARN_VALUE)
		end,
		sync = function ()
			prepare()
			return tostring(LEARN_VALUE)
		end,
	}
}


