-module(merle_cluster).

-export([configure/2, exec/4]).

index_map(F, List) ->
    {Map, _} = lists:mapfoldl(fun(X, Iter) -> {F(X, Iter), Iter +1} end, 1, List),
    Map.

configure(MemcachedHosts, ConnectionsPerHost) ->
    SortedMemcachedHosts = lists:sort(MemcachedHosts),
    DynModuleBegin = "-module(merle_cluster_dynamic).
        -export([get_server/1]).
        get_server(ClusterKey) -> N = erlang:phash2(ClusterKey, ~p), 
            do_get_server(N).\n",
    DynModuleMap = "do_get_server(~p) -> {\"~s\", ~p}; ",
    DynModuleEnd = "do_get_server(_N) -> throw({invalid_server_slot, _N}).\n",
    
    ModuleString = lists:flatten([
        io_lib:format(DynModuleBegin, [length(SortedMemcachedHosts)]),
        index_map(fun([Host, Port], I) -> io_lib:format(DynModuleMap, [I-1, Host, Port]) end, SortedMemcachedHosts),
        DynModuleEnd
    ]),
    
    log4erl:error("dyn module str ~p", [ModuleString]),
    
    {M, B} = dynamic_compile:from_string(ModuleString),
    code:load_binary(M, "", B),

    % start all merle watchers
    lists:foreach(
        fun([Host, Port]) ->
            lists:foreach(
                fun(_) -> 
                    supervisor:start_child(merle_watcher_sup, [[Host, Port]]) 
                end,
                lists:seq(1, ConnectionsPerHost)
            )
        end, 
        SortedMemcachedHosts
    ).


exec(Key, Fun, FullDefault, Now) ->
    S = merle_cluster_dynamic:get_server(Key),

    case merle_pool:get_closest_pid(round_robin, S) of
        in_use ->
            FullDefault;

        P ->
            merle_watcher:monitor(P, self()),
            
            MC = merle_watcher:merle_connection(P),

            Value = Fun(MC, Key),

            merle_watcher:demonitor(P),

            merle_pool:checkin_pid(P, Now),

            Value

    end.