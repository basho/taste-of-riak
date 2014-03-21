%%Copy and past this into your erl shell after starting it with the riak client in the path
%% erl -pa PATH_TO/riak-erlang-client/ebin/ PATH_TO//riak-erlang-client/deps/*/ebin

c(msgy).
rr(msgy).

Joe = #user{user_name="joeuser",
            full_name="Joe User",
            email="joe.user@basho.com"}.

Marleen = #user{user_name="marleenmgr",
                full_name="Marleen Manager",
                email="marleen.manager@basho.com"}.

Msg = msgy:create_msg(Marleen#user.user_name, Joe#user.user_name, "Welcome to the company!").

{ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 10017).
msgy:save_user(Pid, Joe).
msgy:save_user(Pid, Marleen).
msgy:post_msg(Pid, Msg).

{TodaysDate,_} = calendar:now_to_universal_time(erlang:now()).
JoesInboxToday = msgy:get_timeline(Pid, Joe#user.user_name, inbox, TodaysDate).

JoesFirstMessage = msgy:get_msg(Pid, hd(JoesInboxToday#timeline.msgs)).

io:format("From: ~s~nMsg : ~s~n~n", [JoesFirstMessage#msg.sender, JoesFirstMessage#msg.text]).



