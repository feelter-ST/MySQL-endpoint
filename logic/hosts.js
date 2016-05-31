var global_procs = require('./global').procs;

var hosts = {
    servers: [],
    loadcount: 0,
    masterHost: 'feelterdb1.c8k7lolewmtp.us-west-2.rds.amazonaws.com',
    load: function() {
        this.loadcount++;
        var mysql = require('mysql');
        var connection = mysql.createConnection({
            host: this.masterHost,
            user: 'feelter',
            password: 'bl4ck4ndwhite',
            database: 'MidasResearch'
        });
        connection.connect();
        var that=this;
        connection.query('call sp_get_shards', function(err, rows, fields) {
            if (!err) {
                for (var i=0;i<rows[0].length;i++){
                    that.servers.push(rows[0][i]);
                }
            }
            else that.load();
        });
        connection.end();
    },
    hostFromPhrase: function(kp) {
        kp = kp.toLowerCase();
        for (var i = 0; i < this.servers.length; i++) {
            var server = this.servers[i];
            var sKey = server.shardKey;
            var j = sKey.indexOf('2');
            if (j > 0)
            {
                var p1 = sKey.substr(0, j).replace('_', ' ');
                var p2 = sKey.substr(j + 1).replace('_', ' ');
                if (p1 <= kp && (kp.length > p2.length ? kp.substr(0, p2.length) : kp) <= p2)
                    return server.host;
            }
            else
            {
                if (new RegExp(server.rx, 'i').test(kp))
                    return server.host;
            }
        }
    },
    // hostFromCrawlerId: function(req) {
    //     for (var i = 0; i < this.servers.length; i++) {
    //         for (var i1 = 0; i1 < this.servers[i].crawlers.length; i1++) {
    //             if (this.servers[i].crawlers[i1].id == req.headers['x-forwarded-for']) {
    //                 this.servers[i].crawlers[i1].lastAttended = new Date();
    //                 this.servers[i].lastAttended = new Date();
    //                 return this.servers[i].host;
    //             }
    //         }
    //     }
    //     // this crawler is still not assigned
    //     var server = this.leastAttendedServer();
    // },
    isGlobalProc: function(sql) {
        for (var i = 0; i < global_procs.length; i++)
        {
            var proc = global_procs[i];
            if (sql.indexOf(proc) > -1)
                return true;
        }
        console.log('local');
        return false;
    },
    resolveHost: function(req, sql) {
        // test shard override
        var shardKey = req.query.shard || req.body.shard || req.query.shardkey || req.body.shardkey;
        if (shardKey == 'test') {
            console.log('test');
            return 'feelterdbtest.c8k7lolewmtp.us-west-2.rds.amazonaws.com';
        }
        // api request
        if (sql.indexOf('sp_request_keyphrase_data') > -1) {
            var phrase = '';
            var q = req.query.q;
            if (q)
                phrase = q.substr(q.indexOf("'") + 1);
            else if (req.body.qx || req.query.qx)
                phrase = req.body['1'] || req.query['1'];
            var host = this.hostFromPhrase(phrase);
            console.log('request ' + phrase + ' => ' + host.substr(0, host.indexOf('.')));
            return host;
        }
        // crawlers resources requests
        if (this.isGlobalProc(sql)) {
            console.log('global');
            return this.masterHost;
        }
        // this crawlers knows who its working for
        if (shardKey) {
            for (var i = 0; i < this.servers.length; i++) {
                if (this.servers[i].shardKey == shardKey) {
                    console.log(shardKey);
                    return this.servers[i].host;
                }
            }
        }
        // infer the shard from the keyphrase
        var phrase = req.query.phrase || req.body.phrase;
        if (phrase) {
            var host = this.hostFromPhrase(phrase);
            if (host)
            {
                console.log(phrase + ' => ' + host.substr(0, host.indexOf('.')));
                return host;
            }
        }
        console.log('master');
        return this.masterHost;
    }
}
hosts.load();
exports.hosts = hosts;