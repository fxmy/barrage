-module(merge_server).
-behavior(gen_server).

-define(TIMEOUT, 10000).
-record(state, {bukeys_to_merge, pb_pid}).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link(DomainString, Port) ->
	gen_server:start_link({global, ?MODULE}, [DomainString, Port], []).

init([DomainString, Port]) ->
	{ok, Pid} = riakc_pb_socket:start_link(DomainString, Port),
	{ok, #state{bukeys_to_merge = [], pb_pid = Pid}, 0}.

handle_call(_Resquest, _From, State) ->
	{noreply, State, ?TIMEOUT}.

handle_cast(_Msg, State) ->
	{noreply, State, ?TIMEOUT}.

handle_info(timeout, State) ->
	Queue = State#state.bukeys_to_merge,
	Pid = State#state.pb_pid,
	merge_queue(Queue, Pid),
	timer:send_after(?TIMEOUT, timeout),
	{noreply, State#state{bukeys_to_merge = []} };
handle_info({merge_me, {Bucket, Key}}, State) ->
	{noreply, [{Bucket, Key} | State#state.bukeys_to_merge]};
handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internals
merge_queue([], _Pid) ->
	ok;
merge_queue(Queue, Pid) ->
	[{Bucket, _Key}|_] = Queue,
	{_Que_to_merge, Que_rest} = lists:partition( fun({Buck_totest, _Key_totest}) ->
								    filter_out_first(Buck_totest, Bucket) end, Queue),
	%% query 2i to get unmerged keys in bucket, then link them up
	{ok, {_index_results_v1, KeyListtoSort, _index_terms, _continuation}} = riakc_pb_socket:get_index_eq(Pid, Bucket, {integer_index, "processed"}, 0),
	KeyList = lists:sort(KeyListtoSort),
	case KeyList of
		[] -> ok;
		_ ->
			{Bucket, LinkedH, LinkedT} = merge_key(Bucket, KeyList, Pid),
			%% now need to link head&tail infos
			%%{ok, OldHead} = riakc_pb_socket:get(Pid, Bucket, <<"head">>),
			%%OldHKey = riakc_obj:get_value(OldHead),
			case riakc_pb_socket:mapred(Pid, [{Bucket, <<"head">>}], [{link, Bucket, <<"next">>, true}]) of
				{ok, []} ->
					%% brand new bucket, only need to set the head
					set_head(Pid, LinkedH, Bucket);
				{ok, [{Bucket, OldHKey}]} ->
					set_tail(Pid, LinkedT, OldHKey, Bucket),
						%% mark new head in the {Bucket, <<"head">>} 
					set_head(Pid, LinkedH, Bucket)
			end
	end,

	merge_queue(Que_rest, Pid).

set_tail(Pid, TailKey, OldHeadKey, Bucket) ->
	{ok, ObjTail} = riakc_pb_socket:get(Pid, Bucket, TailKey),
	MDTail1 = riakc_obj:get_update_metadata(ObjTail),
	MDTail2 = riakc_obj:set_link(MDTail1, [{<<"next">>, [{Bucket,OldHeadKey}]}]),
	ObjTail2 = riakc_obj:update_metadata(ObjTail, MDTail2),
	riakc_pb_socket:put(Pid, ObjTail2).

set_head(Pid, HeadKey, Bucket) ->
	NewHeadObj = riakc_obj:new(Bucket, <<"head">>, HeadKey),
	MD1 = riakc_obj:get_update_metadata(NewHeadObj),
	MD2 = riakc_obj:set_link(MD1, [{<<"next">>, [{Bucket,HeadKey}]}]),
	NewHeadObj2 = riakc_obj:update_metadata(NewHeadObj, MD2),
	riakc_pb_socket:put(Pid, NewHeadObj2).


filter_out_first( Buck_totest, Bucket) ->
	Bucket == Buck_totest.

%% link-up the keys under the same bucket based on time line
merge_key(_Bucket, [], _Pid) -> {[], []};
merge_key(Bucket, List, Pid) ->
	[H|T] = List,
	merge_key(Bucket, H, H, T, Pid).

merge_key(Bucket, HRlt, Iter, [], _Pid) ->
	{Bucket, HRlt, Iter};
merge_key(Bucket, HRlt, Iter, List, Pid) ->
	[P|N] = List,
	%% do it here
	%% link Iter to P, then continue
	{ok, Obj} = riakc_pb_socket:get(Pid, Bucket, Iter),
	MD1 = riakc_obj:get_update_metadata(Obj),
	MD2 = riakc_obj:set_link(MD1, [{<<"next">>, [{Bucket,P}]}]),
	%% mark as processed	{{integer_index, "processed"}, [1]}
	MD3 = riakc_obj:set_secondary_index(MD2, [{{integer_index, "processed"}, [1]}]),
	Obj2 = riakc_obj:update_metadata(Obj,MD3),
	riakc_pb_socket:put(Pid, Obj2),

	merge_key(Bucket, HRlt, P, N, Pid).
