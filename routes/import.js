/*
 * Crawlers farm back-end
 */
require('epipebomb')();
exports.cdnimport = function(req, res) {
    //res.send("respond with a resource " + req.body.q);
    
    var CRAWLER_IP_PATTERN = '<CrawlerIP>';
    
    //var crawlerIP = req.headers['x-forwarded-for'];
    var crawlerIP = '' + req.connection.remoteAddress;
    //var crawlerIP = req.socket.remoteAddress;
    
    var paramDic = {};
    paramDic[CRAWLER_IP_PATTERN] = crawlerIP;
    
    function traslateParam(p)
    {
        if (p === undefined)
            return null;
        if (paramDic.hasOwnProperty(p))
            return paramDic[p];
        return p;
    }
    
    var xmlEnc = {
        '<': '&lt;',
        '>': '&gt;',
        '&': '&amp;',
        '\'': '&apos;',
        '"': '&quot;',
    };
    
    var dal = require('../logic/dal').dal;
    
    var queryCallback = function(err, rows, fields) {
        if (err) {
            res.end(err.toString());
            return;
        }
        var f = (req.body.f || req.query.f || 'x').toLowerCase();
        try
        {
            switch (f) {
                case 'h':
                    res.end(ToHTMLSimple(rows));
                    break;
                case 'j':
                    res.end('{"rows":' + JSON.stringify(rows) + ',"fields":' + JSON.stringify(fields) + "}");
                    break;
                case 'xs':
                    res.end(ToXmlWithSchema(rows, fields, req.query.qx || req.body.qx));
                    break;
                case 'x':
                default:
                    res.end(rows.length == 0
                        ? '<?xml version="1.0" encoding="utf-8"?><Results></Results>'
                        : ToXML(rows, fields));
            }
        }
        catch (ex) {
            res.end('Format exception [' + f + ']: ' + ex.message);
        }
    };
    
    try {
        var qMain;
        if ((qMain = req.body.y || req.query.y))
            return handle_y(qMain);
        if ((qMain = req.body.m || req.query.m))
            return handle_m(qMain);
        if ((qMain = req.body.q || req.query.q))
            return handle_q(qMain);
        if ((qMain = req.body.qx || req.query.qx))
            return handle_qx(qMain);
        res.end('Unexpected 3');
    }
    catch (ex) {
        res.end("General Exception: " + ex.message);
    }
    
    function handle_y(y) {
        var pass = req.query.p;
        if (!pass || pass != 'bl4ck4ndwhite')
        {
            res.end();
            return;
        }
        var sql = y; //"select json from KeyPhrase where id=4";
        dal.executeOnAll(req, sql, function(err, rows, fields, messages) {
            if (err) {
                res.end('Error while performing query:\n\t' + sql + '\n' + err);
                return;
            }
            try {
                res.end(rows.length == 0
                    ? 'No Results'
                    : ToHTML([rows, []], fields) + '<br/>' + ToHTML([messages, []], fields));
            }
            catch (exml) {
                res.end('render exception: ' + exml.message);
            }
        });
        return;
    }
    
    function handle_m(m) {
        while (m.indexOf(CRAWLER_IP_PATTERN) > -1)
            m = m.replace(CRAWLER_IP_PATTERN, crawlerIP);
        
        var sa = m.split("\r\n");
        var expected = sa.length;
        if (expected == 0) {
            res.end('unexpected 4');
            return;
        }
        
        var r = '';
        var i = -1;
        var executed = 0;

        function doOne() {
            i++;
            if (i >= expected) {
                res.end('x ' + executed + ', e ' + expected); // + ' r ' + r);
                return;
            }
            var s = sa[i];
            var s1 = s.replace("@*@*@", "&");
            if (s1.indexOf(' ') > -1) s1 = s1.replace(' ', '(') + ')';
            s1 = s1.replace(new RegExp('\\\\"', 'gi'), '\\\\\\"');

            var sql = "call sp_" + s1;
            dal.execute(req, sql, function(err, rows, fields) {
                executed++;
                if (err) {
                    res.end('Error while performing Query.' + err + ' ' + r);
                    return;
                }
                try {
                    r += ('x ' + executed + ', e ' + expected + ' sql done: ' + sql + '\r\n'); //ToXML(rows, fields));
                    doOne();
                }
                catch (exml) {
                    res.end('XML Exception (m): ' + exml.message);
                }
            });
        }
        doOne();
    }
    
    function handle_q(q)
    {
        while (q.indexOf(CRAWLER_IP_PATTERN) > -1)
            q = q.replace(CRAWLER_IP_PATTERN, crawlerIP);
        if (q.indexOf(' ') > -1)
            q = q.replace(' ', '(') + ')';
        var sql = "call sp_" + q;
        dal.execute(req, sql, queryCallback);
        return;
    }
    
    function handle_qx(qx)
    {
        if (qx == '' || /[^a-zA-Z_0-9]/.test(qx)) {
            res.end('Error: Illegal procedure name. Procedure names may contain only letters, digits and underscores.');
            return;
        }
        var sql = 'call sp_' + qx + '(';
        var px = [];
        var np = +(req.query[0] || req.body[0]);
        if (np > 0)
        {
            sql += '?' + repeat(",?", np - 1);
            for (var i = 1; i <= np; i++)
                px.push(traslateParam(req.query[i] || req.body[i]));
        }
        sql += ')';
        dal.xcute(req, sql, px, queryCallback);
    }
    
    function escapeXml(unsafe) {
        return unsafe
            .replace(/[<>&'"]/g, function(c) { return xmlEnc[c]; })
            .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, function(c) { return '&#x00' + ('0' + c.charCodeAt(0).toString(16)).slice(-2) + ';'; });
    }

    function repeat(pattern, count) {
        if (count < 1)
            return '';
        var result = '';
        while (count > 1) {
            if (count & 1)
                result += pattern;
            count >>= 1, pattern += pattern;
        }
        return result + pattern;
    }
    
    function ToXML(rows, fields) {
        var r = '<?xml version="1.0" encoding="utf-8"?><Results>';
        for (var it = 0; it < rows.length - 1; it++) {
            var tbl = rows[it];
            var st = 'Table' + (it > 0 ? '_' + it : '');
            for (var i = 0; i < tbl.length; i++) {
                r += '<' + st + '>';
                var row = tbl[i];
                var ks = Object.keys(row);
                for (var ik = 0; ik < ks.length; ik++) {
                    var v = '' + row[ks[ik]];
                    v = escapeXml(v + '');
                    r += '<' + ks[ik] + '>' + v + '</' + ks[ik] + '>';
                }
                r += '</' + st + '>';
            }
        }
        r += '</Results>';
        return r;
    }

    function ToXmlWithSchema(rows, fields, qx) {
        var xml = '<?xml version="1.0" standalone="yes"?>';
        xml += '<' + qx + '>';
        xml += '<xs:schema id="' + qx + '" xmlns="" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata">';
        xml += '<xs:element name="' + qx + '" msdata:IsDataSet="true" msdata:UseCurrentLocale="true">';
        xml += '<xs:complexType>';
        xml += '<xs:choice minOccurs="0" maxOccurs="unbounded">';
        var fieldCount = -1;
        if (fields !== undefined)
            fieldCount += fields.length;
        for (var metaTableIndex = 0; metaTableIndex < fieldCount; metaTableIndex++) {
            var metaTableName = qx + '_' + metaTableIndex;
            xml += '<xs:element name="' + metaTableName + '">';
            xml += '<xs:complexType>';
            xml += '<xs:sequence>';
            var metaTable = fields[metaTableIndex];
            for (var i = 0; i < metaTable.length; i++) {
                var metaField = metaTable[i];
                xml += '<xs:element name="' + metaField['name'] + '" type="xs:string" minOccurs="0" />';
            }
            xml += '</xs:sequence>';
            xml += '</xs:complexType>';
            xml += '</xs:element>';
        }
        xml += '</xs:choice>';
        xml += '</xs:complexType>';
        xml += '</xs:element>';
        xml += '</xs:schema>';
        for (var dataTableIndex = 0; dataTableIndex < rows.length - 1; dataTableIndex++) {
            var detaTable = rows[dataTableIndex];
            var dataTableName = qx + '_' + dataTableIndex;
            for (var i = 0; i < detaTable.length; i++) {
                xml += '<' + dataTableName + '>';
                var dataField = detaTable[i];
                var dataNames = Object.keys(dataField);
                for (var dataIndex = 0; dataIndex < dataNames.length; dataIndex++) {
                    var dataName = dataNames[dataIndex];
                    var dataValue = dataField[dataName];
                    if (dataValue != null)
                        xml += '<' + dataName + '>' + escapeXml('' + dataValue) + '</' + dataName + '>';
                }
                xml += '</' + dataTableName + '>';
            }
        }
        xml += '</' + qx + '>';
        return xml;
    }

    function ToHTML(rows, fields) {
        var r = '<style>body {	background: #fafafa;	color: #444;	font: 100%/30px \'Helvetica Neue\', helvetica, arial, sans-serif;	text-shadow: 0 1px 0 #fff;}strong {	font-weight: bold; }em {	font-style: italic; }table {	background: #f5f5f5;	border-collapse: separate;	box-shadow: inset 0 1px 0 #fff;	font-size: 12px;	line-height: 24px;	text-align: left;}	th {	background: linear-gradient(#777, #444);	border-left: 1px solid #555;	border-right: 1px solid #777;	border-top: 1px solid #555;	border-bottom: 1px solid #333;	box-shadow: inset 0 1px 0 #999;	color: #fff;  font-weight: bold;	padding: 10px 15px;	position: relative;	text-shadow: 0 1px 0 #000;	}th:after {	background: linear-gradient(rgba(255,255,255,0), rgba(255,255,255,.08));	content: \'\';	display: block;	height: 25%;	left: 0;	margin: 1px 0 0 0;	position: absolute;	top: 25%;	width: 100%;}th:first-child {	border-left: 1px solid #777;		box-shadow: inset 1px 1px 0 #999;}th:last-child {	box-shadow: inset -1px 1px 0 #999;}td {	border-right: 1px solid #fff;	border-left: 1px solid #e8e8e8;	border-top: 1px solid #fff;	border-bottom: 1px solid #e8e8e8;	padding: 10px 15px;	position: relative;	transition: all 300ms;}td:first-child {	box-shadow: inset 1px 0 0 #fff;}	td:last-child {	border-right: 1px solid #e8e8e8;	box-shadow: inset -1px 0 0 #fff;}	tr {	background: ;	}tr:nth-child(odd) td {	background: #f1f1f1 ;	}tr:last-of-type td {	box-shadow: inset 0 -1px 0 #fff; }tr:last-of-type td:first-child {	box-shadow: inset 1px -1px 0 #fff;}	tr:last-of-type td:last-child {	box-shadow: inset -1px -1px 0 #fff;}	tbody:hover td {	color: rgba(0,0,0,0.5);	text-snhadow: 0 0 3px #aaa;}tbody:hover tr:hover td {	color: #444;	text-shadow: 0 1px 0 #fff;}</style><table border="1" style="border-collapse: collapse;min-width:100%;" cellpadding="5px">';
        for (var it = 0; it < rows.length - 1; it++) {
            var tbl = rows[it];
            var st = 'Table' + (it > 0 ? '_' + it : '');
            for (var i = 0; i < tbl.length; i++) {
                r += '<tr>';
                var row = tbl[i];
                var ks = Object.keys(row);
                if (i == 0) {
                    r += '<tr>';
                    for (var ik = 0; ik < ks.length; ik++) {
                        r += '<th>' + ks[ik] + '</th>';
                    }
                    r += '</tr>';
                }
                for (var ik = 0; ik < ks.length; ik++) {
                    var v = '' + row[ks[ik]];
                    v = escapeXml(v + '');
                    r += '<td>' + v + '</td>';
                }
                r += '</tr>';
            }
        }
        r += '</table>';
        return r;
    }

    function ToHTMLSimple(tables) {
        //if (rows.length == 0)
        //    return '<table></table>';
        var html = '';//Results:';
        for (var it = 0; it < tables.length - 1; it++)
        {
            var tbl = tables[it];
            if (tbl.length == 0)
                continue;
            html += '\n<table border="1" style="border-collapse: collapse;min-width:100%;" cellpadding="5px">';
            var cols = Object.keys(tbl[0]);
            html += '\n  <tr>';
            for (var ic = 0; ic < cols.length; ic++) {
                html += '\n    <th>' + cols[ic] + '</th>';
            }
            html += '\n  </tr>';
            for (var ir = 0; ir < tbl.length; ir++) {
                html += '\n  <tr>';
                var row = tbl[ir];
                for (var ic = 0; ic < cols.length; ic++) {
                    html += '\n    <td>' + escapeXml('' + row[cols[ic]]) + '</td>';
                }
                html += '\n  </tr>';
            }
            html += '\n</table>';
        }
        return html;
    }
    
}
