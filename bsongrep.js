var _ = require('underscore')
var assert = require('assert')
var argv = require('optimist').argv
var fs = require('fs')
var bson = require('bson').BSONPure.BSON

var rangeFuncs = 
    {
        "$gt": function(a, b){ return a>b; },
        "$gte": function(a, b){ return a>=b; },
        "$lt": function(a, b){ return a<b; },
        "$lte": function(a, b){ return a<=b; },
    }

var arrayFuncs = {
        "regex": function(docfield, target){
            //TODO assert that target is a regex.
            return _.isString(docfield) && target.test(docfield);
        }, 

        "$size": function(docfield, target){
            return _.isArray(docfield) && docfield.length == target
        }, 

        "$in": function(needle, hay){
            if(!_.isArray(hay)){
                //throw an error!
            }else{
                if(_.isArray(needle)){
                    for(var i=0;i<needle.length;i++){
                        if(hay.indexOf(needle[i]) >= 0){
                            return true
                        }
                    }
                    return false;
                }else{
                    return hay.indexOf(needle) >= 0
                }
            }
        }, 

        "$nin": function(needle, hay){
            if(!_.isArray(hay)){
                //throw an error!
            }else{
                if(_.isArray(needle)){
                    for(var i=0;i<needle.length;i++){
                        if(hay.indexOf(needle[i]) >= 0){
                            return false
                        }
                    }
                    return true;
                }else{
                    return hay.indexOf(needle) < 0
                }
            }
        },

        "$all": function(docfield, target){
            if(!_.isArray(target)){
                //error!
            }else{
                if(_.isArray(docfield)){
                    var nummatches = 0;
                    for(var i=0;i<target.length;i++){
                        if(_.indexOf(docfield, target[i])>=0){
                            nummatches++;
                        }
                    }
                    return nummatches == target.length;
                }else{
                    for(var i=0;i<target.length;i++){
                        if(!_.isEqual(target[i], docfield)) return false
                    }
                    return true;
                }
            }
        }


    }

var QueryEngine = function(filters, debug){
    this.filters = filters;
    this.filterFuncs = {}
    this.debug = debug;

    var self = this;

    var constructExactMatcher = function(value){
        return function(fieldVal){
                    if(_.isArray(fieldVal)){
                        for(var i=0;i<fieldVal.length;i++){
                            if(_.isEqual(fieldVal[i], value)){
                                return true;
                            }
                        }
                        return false;
                    }else{
                        return _.isEqual(fieldVal, value);
                    }
                }
    }

    var constructArrayMatcher = function(value, type){
        var functype = arrayFuncs[type];
        return function(fieldVal){
            return arrayFuncs[type](fieldVal, value)
        }
    }

    var constructRangeMatcher = function(value, type){
        var functype = rangeFuncs[type];
        //TODO handle non-matching data types.
        return function(fieldVal){
                    if(_.isArray(fieldVal)){
                        for(var i=0;i<fieldVal.length;i++){
                            if(rangeFuncs[type](fieldVal[i], value)){
                                return true;
                            }
                        }
                        return false;
                    }else{
                        return rangeFuncs[type](fieldVal, value)
                    }
                }
    }


    _.forEach(filters, function(value, key){

        if(_.isObject(value)){
            //TODO if no specials are used, treat this like an exact match on an object

            _.forEach(value, function(qvalue, qkey){
                if(_.indexOf(_.keys(rangeFuncs), qkey) >= 0){
                    // range function
                    if(!self.filterFuncs[key]){
                        self.filterFuncs[key] = []
                    }
                    self.filterFuncs[key].push(constructRangeMatcher(qvalue, qkey));
                }else if(_.indexOf(_.keys(arrayFuncs), qkey) >= 0){
                    // range function
                    if(!self.filterFuncs[key]){
                        self.filterFuncs[key] = []
                    }
                    self.filterFuncs[key].push(constructArrayMatcher(qvalue, qkey));
                }
            });
            // special filters
            // -$gt
            // -$gte
            // -$lt
            // -$lte
            // -$all
            // $exists
            // -$in
            // -$nin
            // $nor
            // $ne
            // $and
            // -$size
            // $type
            // -$regex
            // $elemMatch
            // $not
            // $where
        }else{
            // an exact match only
            if(!self.filterFuncs[key]){
                self.filterFuncs[key] = []
            }
            self.filterFuncs[key].push(constructExactMatcher(value));
        }
    });
}


QueryEngine.prototype.find = function(collection, callback){
    var self = this;
    _.each(collection, function(item){
        var result = true;
        outerloop:
        for(field in self.filterFuncs){
            for(var i=0;i<self.filterFuncs[field].length;i++){
                if(self.debug){
                    console.log(self.filterFuncs[field][i].toString())
                }
                result = result & self.filterFuncs[field][i](item[field])
                if(!result) break outerloop
            }
        }
        if(result){
            callback(item);
        }
    });
}

exports.QueryEngine = QueryEngine


function testHarness(query, collection, debug){
    var testresults = []
    var test = new QueryEngine(query, debug)
    test.find(collection, function(x){testresults.push(x)});
    return testresults;
}

function test(){
    console.log("running tests.")
    //TODO test on array.
    var testcollection = [{"lastname":"smith", "firstname":"will", "id":0}, {"lastname":"joe", "firstname":"joe", "id":1}]
    var testresults = testHarness({"lastname":"smith"}, testcollection)
    assert.equal(testresults.length, 1)
    assert.equal(testresults[0].id, 0)


    //TODO test on array.
    testcollection = [{"lastname":"smith", "age":22, "id":0}, {"lastname":"joe", "firstname":"joe", "age":25}]
    var testresults = testHarness({"age":{"$gt":20}}, testcollection)
    assert.equal(testresults.length, 2)

    //TODO test on array.
    testcollection = [{"test":[1,2,3,4], "age":22, "id":0}, {"test":[5,6,7,8], "age":22, "id":1}]
    var testresults = testHarness({"test":{"$in":[1,5]}}, testcollection);
    assert.equal(testresults.length, 2)
    testresults = testHarness({"test":{"$in":[1]}}, testcollection);
    assert.equal(testresults.length, 1)
    assert.equal(testresults[0].id, 0)
    testresults = testHarness({"test":{"$in":[5]}}, testcollection);
    assert.equal(testresults.length, 1)
    assert.equal(testresults[0].id, 1)


    testcollection = [{x:10, id:0}, {x:[10, 11], id:1}, {x:[1,2,3], id:2}]
    var testresults = testHarness({"x":{"$all":[10]}}, testcollection);
    assert.equal(testresults.length, 2)
    assert.equal(testresults[0].id, 0)
    assert.equal(testresults[1].id, 1)

    testcollection = [{x:10, id:0}, {x:[10, 11], id:1}, {x:[1,2,3], id:2}]
    testresults = testHarness({"x":{"$all":[1,2,3]}}, testcollection);
    assert.equal(testresults.length, 1);
    assert.equal(testresults[0].id, 2);

    testresults = testHarness({"x":{"$size":2}}, testcollection);
    assert.equal(testresults.length, 1)
    assert.equal(testresults[0].id, 1)

    testcollection = [{x:"hey", id:0}, {x:"sup", id:1}, {x:[10], id:2}]
    testresults = testHarness({"x":{"$regex":/^h/}}, testcollection);
    assert.equal(testresults[0].id, 0)
}


function readRecord(fd, lenbuf, callback){
    var lenbuf = new Buffer(4);
    fs.read(fd, lenbuf, 0, 4, null, function(err, bytesRead, buffer){

        if(err){
            console.log(err);
        }else if(bytesRead != 4){
            return;
        }else{ //ok, cool
            var bsonSize = lenbuf.readInt32LE(0);
            //TODO re-use one buffer instead of re-initializing each time.
            var targetBuffer = new Buffer(bsonSize)
            //TODO just do .copy() here instead.
            targetBuffer[0] = lenbuf[0]
            targetBuffer[1] = lenbuf[1]
            targetBuffer[2] = lenbuf[2]
            targetBuffer[3] = lenbuf[3]
            fs.read(fd, targetBuffer, 4, bsonSize-4, null, function(err2, bytesReadDoc){
                if(err2){
                    console.log(err2);
                }else if(bytesReadDoc != bsonSize-4){
                    console.log("expected", bsonSize, "bytes, but got", bytesReadDoc);
                }else{
                    callback(bson.deserialize(targetBuffer));
                }
            });
        }
    });
}

function queryFile(filename, q){
    var lenBuf = new Buffer(4);
    fs.open(filename, 'r', function(err, fd){
        if(err){
            console.log(err);
        }else{
            var repeater = function(){  
                readRecord(fd, lenBuf, 
                    function(doc){
                        repeater();
                    });
            }
            repeater();
        }
    });


}

if(argv.t){
    test()
}
if(argv.f && argv.q){
    queryFile(argv.f, argv.q);
}

