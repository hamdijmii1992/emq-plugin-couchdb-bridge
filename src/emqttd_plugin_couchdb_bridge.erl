%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2017 NGE
%%% @doc
%%% emqttd_plugin_couchdb_bridge.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_plugin_couchdb_bridge).

-include("emqttd/include/emqttd.hrl").

-include("emqttd/include/emqttd_protocol.hrl").

-include("emqttd/include/emqttd_internal.hrl").

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/3, on_client_subscribe_after/3, on_client_unsubscribe/3]).

-export([on_message_publish/2, on_message_delivered/3, on_message_acked/3]).

-record(struct, {lst=[]}).

%% Called when the plugin application start
load(Env) ->
    couchdb_init([Env]),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
    emqttd:hook('client.subscribe.after', fun ?MODULE:on_client_subscribe_after/3, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]).

%%-----------client connect start-----------------------------------%%

on_client_connected(ConnAck, Client = #mqtt_client{client_id  = ClientId}, _Env) ->
    io:format("client ~s connected, connack: ~w~n", [ClientId, ConnAck]),

    Json = jsx:encode([
        {type, <<"connected">>},
        {client_id, ClientId},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),
    
    io:format("on_client_connected ~n~p", list_to_binary(Json)),

    {ok, Client}.

%%-----------client connect end-------------------------------------%%



%%-----------client disconnect start---------------------------------%%

on_client_disconnected(Reason, ClientId, _Env) ->
    io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),

    Json = jsx:encode([
        {type, <<"disconnected">>},
        {client_id, ClientId},
        {reason, Reason},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),

   
    io:format("on_client_disconnected ~n~p", list_to_binary(Json)),

    ok.

%%-----------client disconnect end-----------------------------------%%



%%-----------client subscribed start---------------------------------------%%

%% should retain TopicTable
on_client_subscribe(ClientId, TopicTable, _Env) ->
    io:format("client ~s will subscribe ~p~n", [ClientId, TopicTable]),
    {ok, TopicTable}.
   
on_client_subscribe_after(ClientId, TopicTable, _Env) ->
    io:format("client ~s subscribed ~p~n", [ClientId, TopicTable]),
    
    case TopicTable of
        [_|_] -> 
            %% If TopicTable list is not empty
            Key = proplists:get_keys(TopicTable),
            %% build json to send using ClientId
            Json = jsx:encode([
                {type, <<"subscribed">>},
                {client_id, ClientId},
                {topic, lists:last(Key)},
                {cluster_node, node()},
                {ts, emqttd_time:now_to_secs()}
            ]),
            
            io:format("on_client_subscribe_after ~n~p", list_to_binary(Json)),
        _ -> 
            %% If TopicTable is empty
            io:format("empty topic ~n")
    end,

    {ok, TopicTable}.

%%-----------client subscribed end----------------------------------------%%



%%-----------client unsubscribed start----------------------------------------%%

on_client_unsubscribe(ClientId, Topics, _Env) ->
    io:format("client ~s unsubscribe ~p~n", [ClientId, Topics]),

    % build json to send using ClientId
    Json = jsx:encode([
        {type, <<"unsubscribed">>},
        {client_id, ClientId},
        {topic, lists:last(Topics)},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs()}
    ]),
    
    
     io:format("on_client_unsubscribe ~p~n", list_to_binary(Json)),
    
    {ok, Topics}.

%%-----------client unsubscribed end----------------------------------------%%



%%-----------message publish start--------------------------------------%%

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),   

    From = Message#mqtt_message.from,
    Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload, 
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = jsx:encode([
        {type, <<"published">>},
        {client_id, From},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs(Timestamp)}
    ]),
     {ok, Db} = application:get_env(couchdb, couchdb_instance),
	%{[
     %{<<"_id">>, <<"test">>},
     %{<<"content">>, <<"some text">>}
    %]},
	 couchbeam:save_doc(Db, Json),
     io:format("on_message_publish ~p~n", list_to_binary(Json)),
     {ok, Message}.

%%-----------message delivered start--------------------------------------%%
on_message_delivered(ClientId, Message, _Env) ->
    io:format("delivered to client ~s: ~s~n", [ClientId, emqttd_message:format(Message)]),

    From = Message#mqtt_message.from,
    Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload, 
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = jsx:encode([
        {type, <<"delivered">>},
        {client_id, ClientId},
        {from, From},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs(Timestamp)}
    ]),

    
    io:format("on_message_delivered ~p~n", list_to_binary(Json)),

    {ok, Message}.
%%-----------message delivered end----------------------------------------%%

%%-----------acknowledgement publish start----------------------------%%
on_message_acked(ClientId, Message, _Env) ->
    io:format("client ~s acked: ~s~n", [ClientId, emqttd_message:format(Message)]),   

    From = Message#mqtt_message.from,
    Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload, 
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = jsx:encode([
        {type, <<"acked">>},
        {client_id, ClientId},
        {from, From},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs(Timestamp)}
    ]),

    
    io:format("on_message_acked ~p~n", list_to_binary(Json)),
    {ok, Message}.

%% ===================================================================
%% couchdb_init
%% ===================================================================

couchdb_init(_Env) ->
    %% Get parameters
    {ok, Couchdb} = application:get_env(emqttd_plugin_couchdb_bridge, couchdb),
    CouchDbUrl = proplists:get_value(db_server, Couchdb),
    Username_db= proplists:get_value(username, Couchdb),
	Password_db= proplists:get_value(password, Couchdb),
	
    Options = [{basic_auth, {Username_db, Password_db}}],
    Server = couchbeam:server_connection(CouchDbUrl, Options),
    {ok, _Version} = couchbeam:server_info(Server),
    Db_opts = [],
    wf:info(?MODULE, "Data3: ~p~n",[_Version]),
	Db_Name = proplists:get_value(db_name, Couchdb),
    {ok, Db} = couchbeam:open_db(Server, Db_Name, Db_opts),
	
	
	application:set_env(couchdb, couchdb_instance, Db),
    application:set_env(couchdb, couchdb_url, CouchDbUrl),
    
	application:set_env(couchdb, couchdb_username, Username_db),
    
    application:set_env(couchdb, couchdb_password, Password_db),

    {ok, _} = application:ensure_all_started(couchdb),

    io:format("Init couchdb with ~p~n", [CouchDbUrl]).


%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
    emqttd:unhook('client.subscribe.after', fun ?MODULE:on_client_subscribe_after/3),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/3),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/3).

