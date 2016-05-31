var industryPatterns=[
    {name:"edmunds / cars",type:"rx",industry:230, pattern:"edmunds\\.com"}
    ]

var industry = {
    resolve: function(sourceurl, phrase) {
        try {
            for(var i=0;i<industryPatterns.length;i++){
                var p=industryPatterns[i];
                switch (p.type){
                    case 'rx':{
                        if(new RegExp(p.pattern,'gi').test(sourceurl)) return p.industry;       
                    }
                }
            }       
        }
        catch (e) {
            console.log(e.message + ' ' + JSON.stringify(e));
        }
        return 'NULL';
    }
}

exports.industry = industry;