-module(merle_sup).

-export([start_link/2, init/1]).

-export([server_name/2]).

-behaviour(supervisor).

start_link(Instances, ConnectionsPerInstance) ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, [Instances, ConnectionsPerInstance]),

    %local_pg2:start(),
    merle_cluster:configure(Instances, ConnectionsPerInstance),

    {ok, Pid}.

init([Instances, ConnectionsPerInstance]) ->

    PBChildren = poolboy_children(Instances, ConnectionsPerInstance),
    
    {ok, {{one_for_one, 10, 10}, PBChildren}}.

poolboy_children(Instances, ConnectionsPerInstance) -> 
    poolboy_children(Instances, ConnectionsPerInstance, []).
    
poolboy_children([], _ConnectionsPerInstance, Acc) -> Acc;
poolboy_children([[Host, Port] | Rest], ConnectionsPerInstance, Acc) ->
    PBName = server_name(Host, Port),

    PBChild = poolboy:child_spec(
        PBName,
        [
            {name, {local, PBName}}, 
            {worker_module, merle}, 
            {size, ConnectionsPerInstance}, 
            {max_overflow, ConnectionsPerInstance}
        ], 
        [Host, Port]),

    poolboy_children(Rest, ConnectionsPerInstance, [ PBChild | Acc ]).

server_name(Host, Port) ->
    list_to_atom("pb_" ++ Host ++ "_" ++ integer_to_list(Port)).