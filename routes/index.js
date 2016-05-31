/*
 * API end point
 */
var industry = require('../logic/industry').industry;
exports.index = function(req, res) {
  try {
    if (req.query.q) {
      if (req.query.ref){
        req.headers.refid=req.query.ref;
      }
      else if (req.headers.referer){
        req.headers.refid=req.headers.referer;
      }
    	else req.headers.refid = req.headers['x-forwarded-for'] || req.connection.remoteAddress;      

      var q=req.query.q.replace(new RegExp('\'', 'gi'), '\'\'').split('|')[0];
      var dal = require('../logic/dal').dal;
      var industryid=industry.resolve(req.headers.refid);
      dal.execute(req, 'call sp_request_keyphrase_data(\'' + q + '\',' + industryid + ',\'v1-'+req.headers.refid+'\')', function(err, rows, fields) {
        if (!err){
          var j=rows[0][0][fields[0][0].name];
          if (req.query.callback) j=req.query.callback+'(['+j+']); // shard';
          res.end(j);
        }
        else
          res.end('Error while performing Query.'+err);
      });
    }
    else {
      res.end('q varable missing');
    }
  }
  catch (e) {
    res.end(e.message);
  }
};