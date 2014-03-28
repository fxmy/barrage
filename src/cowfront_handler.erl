-module(cowfront_handler).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

init(_Transport, Req, []) ->
	{ok, Req, undefined}.

handle(Req, State) ->
	{Method, Req2} = cowboy_req:method(Req),
	HasBody = cowboy_req:has_body(Req2),
	{ok, Req3} = maybe_update(Method, HasBody, Req2),
	{ok, Req3, State}.

maybe_update(<<"POST">>, true, Req) ->
	{ok, PostVals, Req2} = cowboy_req:body_qs(Req),

	%%Channel::binary()
	Path = cowboy_req:path(Req2),
	case Path of
		<<"/">> -> Channel = undefined;
		PathBin ->
			Channel = binary:part(PathBin, {1,byte_size(PathBin)-1})
	end,
	
	%% Comment::binary()|no_comment
	case proplists:get_value(<<"comment">>, PostVals) of
		undefined -> Comment = no_comment;
		CommBin -> Comment = CommBin
	end,

	%%IP
	{{FromIP, _FromPort}, _Req} = cowboy_req:peer(Req2),

	%% need verify
	%% UserID::binary()
	UserID = cowboy_req:cookie(<<"userid">>, Req2),

	%%LastTimeStamp::{MegaSec, Sec, 0}
	case proplists:get_value(<<"lasttime">>, PostVals) of
		undefined ->
			LastTimeStamp = no_time;
		LastTimeBin ->
			MegaSecBin = binary:part(LastTimeBin, {0,4}),
			SecBin = binary:part(LastTimeBin, {4,6}),
			LastTimeStamp = {binary_to_integer(MegaSecBin),SecBin,0}
	end,

	update({Channel, LastTimeStamp, UserID, FromIP, Comment}, Req2);
maybe_update(<<"POST">>, false, Req) ->
	cowboy_req:reply(400, [], <<"Missing body.">>, Req);
maybe_update(_, _, Req) ->
	%% Method not allowed.
	cowboy_req:reply(405, Req).

update({undefined, _LastTimeStamp, _UserID, _FromIP, _Comment}, Req) ->
	cowboy_req:reply(400, [], <<"Missing Channel.">>, Req);
update({_Channel, _LastTimeStamp, undefined, _FromIP, _Comment}, Req) ->
	cowboy_req:reply(400, [], <<"Missing UID.">>, Req);
update({Channel, LastTimeStamp, UserID, FromIP, Comment}, Req) ->
	%% get channel_server from dispatcher, then call the server.
	case barr_dispatcher:get_channel(Channel) of
		{failStartChannel, Channel} ->
			cowboy_req:reply(503, [], <<"Channel Server fail.">>, Req);
		{ok, PidChannel} ->
			CommenList = barr_channel_server:update(PidChannel, {LastTimeStamp, UserID, FromIP, Comment}),
			Contents = [Content || {_PostTime,_UID,_FromIP,Content} <- CommenList],
			ContAll = lists:mapfoldl( fun(X,All)->{X,<<X/binary,<<";">>/binary,All/binary>>} end, <<>>, Contents),
			cowboy_req:reply(200, [], ContAll, Req)
	end;
update(Echo, Req) ->
	cowboy_req:reply(200, [
		{<<"content-type">>, <<"text/plain; charset=utf-8">>}
	], Echo, Req).

terminate(_Reason, _Req, _State) ->
	ok.
