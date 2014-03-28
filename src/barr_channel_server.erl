-module(barr_channel_server).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TIMEOUT, 20000).

%% current_state should be init|running
-record(state, {channel_name, max_comments, online_num, comment_queue, comment_num, comment_reserved, current_state}).

%% API
-export([start_link/2, update/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link(ChannelName, MaxComments) ->
	gen_server:start_link(?MODULE, [ChannelName, MaxComments], []).

update(Pid, {LastTime, UserID, FromIP, PostedComment}) ->
	gen_server:call(Pid, {update, {LastTime, UserID, FromIP, PostedComment}}).
	

%% gen_server callbacks
init([ChannelName, MaxComments]) ->
	%% Immediately inform barr_dispatcher I'm ready cause we don't dispatcher be blocked
	%% Also used to update barr_dispatcher Channel_name & pid corresponding incase restarted by barr_channel_sup
	%% Use plain send(!) to avoid circular func calls

	barr_dispatcher ! {channelStarted,ChannelName, self()},

	{ok, #state{channel_name=ChannelName, max_comments=MaxComments, online_num=0, current_state=init}, 0}.

%% new User need all the comments
handle_call({update, {no_time, _UserID, _FromIP, no_comment}}, _From, State) ->
	CommentQueue = State#state.comment_queue,
	L = queue:to_list(CommentQueue),

	{reply, L, State, ?TIMEOUT};
%% heart beat sync from User
handle_call({update, {LastTime, _UserID, _FromIP, no_comment}}, _From, State) ->
	CommentQueue = State#state.comment_queue,
	L = queue:to_list(CommentQueue),
	Update = [ Entry || Entry = {LasT, _, _, _} <- L, LasT > LastTime],

	{reply, Update, State, ?TIMEOUT};
%% User posted a comment when comment_queue is full
handle_call({update, {LastTime, UserID, FromIP, PostedComment}}, _From, State) when State#state.comment_num >= State#state.max_comments ->
	CommentQueue = State#state.comment_queue,
	ChannelName = State#state.channel_name,
	L = queue:to_list(CommentQueue),
	Update = [ Entry || Entry = {PostT, _, _, _} <- L, PostT > LastTime],
	
	PostTime = os:timestamp(),
	Comm = {ChannelName, PostTime, UserID, FromIP, PostedComment},
	barr_db_interface:new_comment(Comm),
	{_, Q1} = queue:out_r(CommentQueue),
	NewCommentQueue = queue:in_r({PostTime, UserID, FromIP, PostedComment}, Q1),

	io:format("new comment when FULL: ~p~nNums: ~p~n", [NewCommentQueue, State#state.comment_num]),

	{reply, Update, State#state{comment_queue = NewCommentQueue}, ?TIMEOUT};
%% User posted a comment while comment_queue is NOT full
handle_call({update, {LastTime, UserID, FromIP, PostedComment}}, _From, State) when State#state.comment_num < State#state.max_comments ->
	CommentQueue = State#state.comment_queue,
	CommentNum = State#state.comment_num + 1,
	ChannelName = State#state.channel_name,
	L = queue:to_list(CommentQueue),
	Update = [ Entry || Entry = {PostT, _, _, _} <- L, PostT > LastTime],

	PostTime= os:timestamp(),
	Comm = {ChannelName, PostTime, UserID, FromIP, PostedComment},
	barr_db_interface:new_comment(Comm),
	NewCommentQueue = queue:in_r({PostTime, UserID, FromIP, PostedComment}, CommentQueue),

	io:format("new comment when NOT FULL: ~p~nNums: ~p~n", [NewCommentQueue, CommentNum]),


	{reply, Update, State#state{comment_queue = NewCommentQueue, comment_num = CommentNum}, ?TIMEOUT};
handle_call(_Request, _From, State) ->
	{noreply, State, ?TIMEOUT}.

handle_cast(_Msg, State) ->
	{noreply, State, ?TIMEOUT}.

handle_info(timeout, State) ->
	%%io:format("timeout!!!!~n"),
	case State#state.current_state of
		init ->
			{ok, {CommentList, Length, ReservedComments}} = barr_db_interface:get_comments(State#state.channel_name, State#state.max_comments),
			CommentQueue = queue:from_list(CommentList),
			
			%% **Length represents the length of CommentList WITHOUT the count of ReservedComments**
			{noreply, State#state{comment_queue = CommentQueue, comment_num = Length, comment_reserved = ReservedComments, current_state = running}, ?TIMEOUT};
		running ->
			{stop, normal, State}
	end;
handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	io:format("~nChannel_server ~p(~p) exited~n",[_State#state.channel_name, self()]),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
