-module(post_comm_notify).

-export([notify/1]).

notify(Object) ->
	error_logger:info_msg("Post-commiting: ~p~n", [Object]),
	Bucket = riak_object:bucket(Object),
	Key = riak_object:key(Object),
	Pid = global:whereis_name(merge_comments),
	error_logger:info_msg("FFFFFFFFFFFFF: ~p~n~p~n~p~n",[Bucket, Key, Pid]),
	case Pid of
		undefined ->
			error_logger:info_msg("can't find merge serer~n");
		Pid ->
			Pid ! {merge_me, {Bucket, Key}}
	end.
