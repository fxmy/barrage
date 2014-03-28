-module(barr_dispatcher).
-behaviour(gen_server).

%% API
-export([start_link/0, get_channel/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DISPATCH_TAB, barr_dispatch).

%% API
get_channel(Channel_name) ->
	gen_server:call(?SERVER, {get_channel, {Channel_name, 100}}).



start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%% @doc set up ets table to map channelName to channel_server pids, we use named_table so other processes can directly lookup the table.
%% @end
init([]) ->
	Tab = ets:new(?DISPATCH_TAB, [named_table, {read_concurrency, true}]),
	{ok, Tab}.

handle_call({get_channel, {Channel_name, MaxComments}}, _From, State)->
	%%first check whether the process really exists
	case ets:lookup(State, Channel_name) of
		[{Channel_name, Pid, _MoniRef}] ->
			{reply, {ok, Pid}, State};
		[] ->
			barr_channel_sup:start_child({Channel_name, MaxComments}),
			%% when channel started, we should receive
			receive
				{channelStarted, Channel_name, NewPid} ->
					MonRef = monitor(process, NewPid),
					ets:insert(State, {Channel_name, NewPid, MonRef}),
					{reply, {ok,NewPid}, State}
			after 200 ->
					{reply, {failStartChannel, Channel_name}, State}
			end
	end;
handle_call(_Msg, _From, State) ->
	{noreply, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({channelStarted, Channel_name, Pid}, State) ->
	%% this should happen when a barr_channel_server process is restarted by supervisor
	MonRef = monitor(process, Pid),
	ets:insert(State, {Channel_name, Pid, MonRef}),
	{noreply,  State};
handle_info({'DOWN', _Ref, process, Pid, normal}, State) ->
	%% barr_channel_server exits normally
	ets:match_delete(State, {'_', Pid, '_'}),
	{noreply, State};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
