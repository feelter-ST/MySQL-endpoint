/*
 * Crawlers farm back-end
 */
require('epipebomb')();
exports.checksum = function(req, res) {
    var fs = require('fs'); // file system module
    //var basePath = "/home/ubuntu/workspace/"; // development (Cloud9)
    var basePath = "/home/ec2-user/feelter/public/downloads/"; // production
    var crypto = require('crypto'); // cryptography module
    var q = req.query.q || req.body.q || '';
    var fList = q.split('|');
    var fCount = fList.length;
    var resList = [];
    var checkDone = function(fName, fErr, fHash) {
        var fRes = {};
        fRes['name'] = fName;
        fRes['err'] = fErr;
        fRes['hash'] = fHash;
        resList.push(fRes);
        if (resList.length == fCount)
            res.end(JSON.stringify(resList));
    };
    var rsOnEnd = function(fName, cHash) {
        return function () {
            checkDone(fName, null, cHash.digest('hex'));
        }
    };
    var rsOnError = function(fName) {
        return function (err) {
            checkDone(fName, err, null);
        }
    };
    var rsOnData = function(cHash) {
        return function (data) {
            cHash.update(data, 'utf8');
        };
    };
    var FILE_NAME_REGEX = /^\w+(\.\w+)*$/;
    var isValid = function(fName) {
        return FILE_NAME_REGEX.test(fName);
    };
    fList.forEach(function(fName, i, fNameList) {
        try {
            fName = fName.trim();
            if (isValid(fName) == false)
                return checkDone(fName, "invalid file name", null);
            var filePath = basePath + fName;
            if (fs.existsSync(filePath) == false)
                return checkDone(fName, "file not found", null);
            var hash = crypto.createHash('md5');
            var readStream = fs.createReadStream(filePath);
            readStream.on('error', rsOnError(fName));
            readStream.on('data', rsOnData(hash));
            readStream.on('end', rsOnEnd(fName, hash));
        }
        catch (ex) {
            return checkDone(fName, ex, null);
        }
    })
}
