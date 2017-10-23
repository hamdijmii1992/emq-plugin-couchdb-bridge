%%% @doc
%%% emq_plugin_couchdb_bridge.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emq_plugin_couchdb_bridge_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Sup} = emq_plugin_couchdb_bridge_sup:start_link(),
    emq_plugin_couchdb_bridge:load(application:get_all_env()),
    {ok, Sup}.

stop(_State) ->
    emq_plugin_couchdb_bridge:unload().

