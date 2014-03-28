-module(barrage_sup).
-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).


start_link() ->
	supervior:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
	RestartStrategy = {one_for_one, 10, 30},
	ChannelSup = {barr_channel_sup, {barr_channel_sup, start_link, []},
		permanent, infinity, supervisor, [channel_sup]},
	Dispatcher = {barr_dispatcher, {barr_dispatcher, start_link, []},
		permanent, infinity, worker, [dispatcher]},
	Children = [ChannelSup, Dispatcher],
	{ok, {RestartStrategy, Children}}.
