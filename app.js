
/**
 * Module dependencies.
 */

var express = require('express')
  , routes = require('./routes')
  , cdnimport = require('./routes/import')
  , checksum = require('./routes/checksum')
  , http = require('http')
  , path = require('path');
;

var app = express();

// all environments
app.set('port', process.env.PORT || 80);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));


// development only v2
if ('development' == app.get('env')) {
  app.use(express.errorHandler());
}

app.get('/', routes.index);
app.post('/import.ashx', cdnimport.cdnimport);
app.get('/import.ashx', cdnimport.cdnimport);
app.post('/checksum.ashx', checksum.checksum);
app.get('/checksum.ashx', checksum.checksum);

http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});
