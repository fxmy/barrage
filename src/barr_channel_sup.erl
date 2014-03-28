-module(barr_channel_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, start_child/1]).

%% supervisor callback
-export([init/1]).

-define(SERVER, ?MODULE).


start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).



start_child({ChannelName, MaxComments}) ->
	supervisor:start_child(?SERVER, [ChannelName, MaxComments]).

init([]) ->
	RestartStrategy = {simple_one_for_one, 0, 1},
	Server = {barr_channel_server, {barr_channel_server, start_link, []},
		temporary, 2000, worker, [channel_server]},
	Children = [Server],
	{ok, {RestartStrategy, Children}}.
