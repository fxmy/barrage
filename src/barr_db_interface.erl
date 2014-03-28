-module(barr_db_interface).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/2, get_comments/2, new_comment/1]).

-record(state, {pb_pid}).

-define(SERVER, ?MODULE).

%% APIs
start_link(DomainString, Port) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [DomainString, Port], []).

get_comments(Channel_name, MaxComments) ->
	gen_server:call(?SERVER, {get_comments, Channel_name, MaxComments}).

new_comment(Comment) ->
	gen_server:cast(?SERVER, {new_comment, Comment}).

%% gen_server callbacks
init([DomainString, Port]) ->
	{ok, Pid} = riakc_pb_socket:start_link(DomainString, Port),
	{ok, #state{pb_pid = Pid}}.
handle_call({get_comments, Channel_name, MaxComments}, _From, State) ->
	LinkPhase = [{link, Channel_name, <<"next">>, true}||_<- lists:seq(1,MaxComments)],
	{ok, BuKePairs} = riakc_pb_socket:mapred(State#state.pb_pid,[{Channel_name, <<"head">>}],LinkPhase),
	ValueList = [ riakc_obj:get_value( glue( riakc_pb_socket:get(State#state.pb_pid, Bu, Ke))) || {_PhaseNo,[[Bu,Ke,_LinkTag]]} <- BuKePairs],
	
	{reply, {ok, {ValueList,erlang:length(ValueList),[]} }, State};
handle_call(_Request, _From, State) ->
	{noreply, State}.

handle_cast({new_comment, Comment}, State) ->
	{ChannelName, TimeStamp, UserID, FromIP, PostedComment} = Comment,
	{MegaSec, Sec, MicroSec} = TimeStamp,
	Object1 = riakc_obj:new(ChannelName, term_to_binary({MegaSec, Sec, MicroSec, UserID}), PostedComment),
	MD1 = riakc_obj:get_update_metadata(Object1),
	MD2 = riakc_obj:set_secondary_index(MD1, [{{binary_index, "time"}, [term_to_binary(TimeStamp)]}, {{binary_index, "ip"}, [term_to_binary(FromIP)]}, {{integer_index, "processed"}, [0]}, {{integer_index, "reserved"}, [0]} ]),
	Object2 = riakc_obj:update_metadata(Object1, MD2),
	riakc_pb_socket:put(State#state.pb_pid, Object2),
	{noreply, State};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

glue({_ok,V}) -> V.
