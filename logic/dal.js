var hosts = require('./hosts').hosts;

var dal = {
    execute: function(req, sql, callback) {
        try {
            var host = hosts.resolveHost(req, sql);
            //callback('host:'+host);
            //return;
            var mysql = require('mysql');
            var connection = mysql.createConnection({
                host: host,
                user: 'feelter',
                password: 'bl4ck4ndwhite',
                database: 'MidasResearch',
                supportBigNumbers: true,
                bigNumberStrings: true
            });

            connection.connect();

            connection.query(sql, callback);
            connection.end();
        }
        catch (e) {
            callback(e.message + ' host: ' + host + ' ' + JSON.stringify(e));
        }
    },
    executeOnAll: function(req, sql, callback) {
        try {
            var count = hosts.servers.length;
            var returned = 0;
            var allrows = [];
            var allmessages=[];
            for (var i = 0; i < count; i++) {
                var host = hosts.servers[i].host;
                //callback('host:'+host);
                //return;
                var mysql = require('mysql');
                var connection = mysql.createConnection({
                    host: host,
                    user: 'feelter',
                    password: 'bl4ck4ndwhite',
                    database: 'MidasResearch',
                    supportBigNumbers: true,
                    bigNumberStrings: true
                });

                connection.connect();

                connection.query(sql, function(err, rows, fields) {
                    try {
                        returned++;
                        if (!err) {
                            if (rows.length > 0) {
                                for (var ir = 0; ir < rows.length; ir++) rows[ir].shard = this._connection.config.host.split('.')[0];
                                allrows = allrows.concat(rows);
                            }
                            allmessages.push({shard:this._connection.config.host.split('.')[0],info:'rows: '+rows.length});
                        }
                        else
                            allmessages.push({shard:this._connection.config.host.split('.')[0],info:'Error while performing Query.' + err + ' shard: ' + host + ' ' + sql});
                    }
                    catch (e) {
                        allmessages.push({shard:this._connection.config.host.split('.')[0],info:'Error while performing Query.' + e + ' shard: ' + host + ' ' + sql});
                    }
                    if (returned == count) {
                        function IsNumeric(n) {
                            return !isNaN(parseFloat(n)) && isFinite(n);
                        }
                        if (sql.toLowerCase().indexOf('order by') > -1) {
                            var ob = sql.substring(sql.toLowerCase().indexOf('order by') + 9).split(',');
                            for (var io = 0; io < ob.length; io++) {
                                var asr = ob[io].trim().split(' ');
                                if (asr[0].toLowerCase() == 'limit') break;
                                allrows.sort(function(a, b) {
                                    if (IsNumeric(a[asr[0]]) && IsNumeric(b[asr[0]])) {
                                        if (asr.length > 0 && asr[1].toLowerCase() == 'desc') return parseFloat(b[asr[0]]) - parseFloat(a[asr[0]]);
                                        else return parseFloat(a[asr[0]]) - parseFloat(b[asr[0]]);
                                    }
                                    else {
                                        var prop = asr[0];
                                        if (asr.length > 0 && asr[1].toLowerCase() == 'desc') {
                                            if (b[prop] > a[prop]) {
                                                return 1;
                                            }
                                            else if (b[prop] < a[prop]) {
                                                return -1;
                                            }
                                            return 0;
                                        }
                                        else {
                                            if (a[prop] > b[prop]) {
                                                return 1;
                                            }
                                            else if (a[prop] < b[prop]) {
                                                return -1;
                                            }
                                            return 0;
                                        }
                                    }
                                });
                            }
                        }
                        if (sql.toLowerCase().indexOf('limit') > -1) {
                            var ol = sql.substring(sql.toLowerCase().indexOf('limit') + 6).split(',')[0];
                            allrows.splice(parseFloat(ol),allrows.length-parseFloat(ol));
                        }
                        //allrows.push({messages:'messages:'});
                        allmessages.sort(function(a,b){return parseInt(a.shard.replace('feelterdb',''))-parseInt(b.shard.replace('feelterdb',''))});
                        //allrows = allrows.concat(allmessages.sort(function(a,b){return parseInt(a.shard.replace('feelterdb',''))-parseInt(b.shard.replace('feelterdb',''))}));
                        callback(err, allrows, fields, allmessages)
                    }
                });
                connection.end();
            }
        }
        catch (e) {
            callback(e.message + ' host: ' + host + ' ' + JSON.stringify(e));
        }
    },
    xcute: function(req, qx, px, callback) {
        try {
            var host = hosts.resolveHost(req, qx);
            var mysql = require('mysql');
            var connection = mysql.createConnection({
                host: host,
                user: 'feelter',
                password: 'bl4ck4ndwhite',
                database: 'MidasResearch',
                supportBigNumbers: true,
                bigNumberStrings: true
            });
            connection.connect();
            connection.query(qx, px, callback);
            //connection.end();
        }
        catch (e) {
            callback(e.message + ' host: ' + host + ' ' + JSON.stringify(e));
        }
        connection.end();
    }
}

exports.dal = dal;