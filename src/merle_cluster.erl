-module(merle_cluster).

-export([configure/2, exec/4]).

index_map(F, List) ->
    {Map, _} = lists:mapfoldl(fun(X, Iter) -> {F(X, Iter), Iter +1} end, 1, List),
    Map.

configure(MemcachedHosts, ConnectionsPerHost) ->
    SortedMemcachedHosts = lists:sort(MemcachedHosts),
    DynModuleBegin = "-module(merle_cluster_dynamic).
        -export([get_connections_per_host/0, get_server/1]).
        get_connections_per_host() -> ~p.
        get_server(ClusterKey) -> N = erlang:phash2(ClusterKey, ~p), 
            do_get_server(N).\n",

    DynModuleMap = "do_get_server(~p) -> {\"~s\", ~p}; ",
    DynModuleEnd = "do_get_server(_N) -> throw({invalid_server_slot, _N}).\n",

    
    ModuleString = lists:flatten([
        io_lib:format(DynModuleBegin, [ConnectionsPerHost, length(SortedMemcachedHosts)]),
        index_map(fun([Host, Port], I) -> io_lib:format(DynModuleMap, [I-1, Host, Port]) end, SortedMemcachedHosts),
        DynModuleEnd
    ]),

    lager:info("dyn module str ~p", [ModuleString]),

    {M, B} = dynamic_compile:from_string(ModuleString),
    code:load_binary(M, "", B),

    % start all merle watchers
    lists:foreach(
        fun([Host, Port]) ->
            lists:foreach(
                fun(Index) ->
                    merle_client_sup:start_child([Host, Port, Index])
                end,
                lists:seq(1, ConnectionsPerHost)
            )
        end, 
        SortedMemcachedHosts
    ).


exec(Key, Fun, Default, Now) ->
    NumConnections = merle_cluster_dynamic:get_connections_per_host(),
    S = merle_cluster_dynamic:get_server(Key),
    exec_on_client(
        merle_pool:get_client(round_robin, S, NumConnections),
        Key,
        Fun,
        Default,
        Now
    ).

exec_on_client({error, Error}, _Key, _Fun, Default, _Now) ->
    lager:warning("Error finding merle client: ~p, returning default value", [Error]),
    {Error, Default};
exec_on_client(undefined, _Key, _Fun, Default, _Now) ->
    lager:warning("Undefined merle client, returning default value"),
    {undefined_client, Default};
exec_on_client({client, _Client, true}, _Key, _Fun, Default, _Now) ->
    lager:info("Merle client is checked out"),
    {busy, Default};
exec_on_client({client, Client, _}, Key, Fun, Default, Now) ->
    exec_on_socket(merle_client:checkout(Client, self(), Now), Client, Key, Fun, Default).

exec_on_socket(no_socket, _Client, _Key, _Fun, Default) ->
    lager:info("Designated merle connection has no socket, returning default value"),
    {busy, Default};
exec_on_socket(busy, _Client, _Key, _Fun, Default) ->
    lager:info("Designated merle connection is in use, returning default value"),
    {in_use, Default};
exec_on_socket(Socket, Client, Key, Fun, Default) ->
    FinalValue = case Fun(Socket, Key) of
        {error, Error} ->
            lager:info("Merle encountered error ~p, returning default value", [Error]),
            {Error, Default};
        {ok, Value} ->
            {ok, Value}
    end,

    merle_client:checkin(Client),

    FinalValue.

