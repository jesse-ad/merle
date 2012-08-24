-module(merle_sup).

-export([start_link/2, init/1]).

-behaviour(supervisor).

start_link(Instances, ConnectionsPerInstance) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Instances, ConnectionsPerInstance]).

init([Instances, ConnectionsPerInstance]) ->
    MerlePool = 
        {merle_pool, 
            {merle_pool, start_link, []},
            permanent, 5000, worker, dynamic
        },

    MerleWatcherSup = 
        {merle_watcher_sup, 
            {merle_watcher_sup, start_link, [Instances, ConnectionsPerInstance]},
            permanent, 5000, supervisor, dynamic
        },

    {ok, {{one_for_all, 10, 10}, [MerlePool, MerleWatcherSup]}}.