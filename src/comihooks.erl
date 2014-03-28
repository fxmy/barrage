-module(comihooks).

-compile(export_all).
merge([]) ->
	{[],[]};
merge(List) ->
	[H|T] = List,
	merge(H, H, T).

merge(HRlt, Iter, []) ->
	{HRlt, Iter};
merge(HRlt, Iter, List) ->
	[P|N] = List,
	io:format("~p~p~p~n", [ Iter, " to " ,P]),
	merge(HRlt, P, N).
