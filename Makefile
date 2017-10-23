PROJECT = emqttd_plugin_couchdb_bridge
PROJECT_DESCRIPTION = EMQ Custom Plugin
PROJECT_VERSION = 1.0

BUILD_DEPS = emqttd
dep_emqttd = git https://github.com/emqtt/emqttd master

COVER = true

include erlang.mk

app:: rebar.config