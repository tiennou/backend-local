var q = require('q'),
    _ = require('lodash'),
    utils = require('../../utils'),
    common = require('@screeps/common'),
    config = common.configManager.config.backend,
    db = common.storage.db,
    env = common.storage.env;



const ROOM_MAP_CHANNEL_RE = /^roomMap2:([WE]\d+[NS]\d+)$/;

module.exports = function(listen, emit) {

    var connectedToRooms = Object.create(null), m;

    listen(/^roomsDone$/, _.throttle(() => {

        var roomsToFetch = [], roomsIdx = Object.create(null);

        for(let roomName in connectedToRooms) {
            if (connectedToRooms[roomName].length > 0) {
                roomsIdx[roomName] = roomsToFetch.length;
                roomsToFetch.push(env.keys.MAP_VIEW+roomName);
            }
        }

        if(!roomsToFetch.length) {
            return;
        }

        env.mget(roomsToFetch).then((mapViewData) => {

            for(let roomName in connectedToRooms) {

                let mapView = mapViewData[roomsIdx[roomName]] || "{}";
                let message = `["roomMap2:${roomName}",${mapView}]`;

                connectedToRooms[roomName].forEach((i) => {
                    i.conn._writeEventRaw(message);
                });
            }

        })

    }, config.socketUpdateThrottle));

    return {
        onSubscribe(channel, user, conn) {

            if(user && (m = channel.match(ROOM_MAP_CHANNEL_RE))) {

                let roomName = m[1];

                connectedToRooms[roomName] = connectedToRooms[roomName] || [];
                connectedToRooms[roomName].push({
                    conn,
                    user
                });

                var startTime = Date.now();

                env.get(env.keys.MAP_VIEW+roomName).then((data) => {
                    data = data || "{}";
                    conn._writeEventRaw(`["roomMap2:${roomName}",${data}]`);
                });

                conn.on('close', () => {
                    if(connectedToRooms[roomName]) {
                        _.remove(connectedToRooms[roomName], (i) => i.conn === conn);
                    }
                });

                return true;
            }

            return false;
        },

        onUnsubscribe(channel, user, conn) {

            if(m = channel.match(ROOM_MAP_CHANNEL_RE)) {
                if(connectedToRooms[m[1]]) {
                    _.remove(connectedToRooms[m[1]], (i) => i.conn === conn);
                }
            }
        }
    };


};