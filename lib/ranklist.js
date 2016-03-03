/**
 * FileName: ranklist
 * Created by "Justin Chou <zhou78620051@126.com>".
 * On Date: 8/6/2014.
 * At Time: 10:31 AM
 */

var redis = require('lvge-redis').classic;
var Utils = require('lvge-utils');
var logger = require('pomelo-logger').getLogger(__filename, "Module LvGe-RankList");
var async = require("async");
var fs = require("fs");

var redisName = "RankList";

/**
 * 配置数据库信息
 * @param {String} config 配置文件路径
 */
function configure(config) {
    // 如果为文件路径. 加载文件
    if (typeof config === 'string') {
        config = Utils.UString.readConfig(config);    // JSON.parse(fs.readFileSync(config, 'utf8'));
    }
    var opts = {
        'name': redisName,
        'host': config.host,
        'port': config.port,
        'proxy': config.proxy
    };
    if (config.auth_pass){
        opts.auth_pass = config.auth_pass;
    }
    if (config.debug_mode) {
        opts.debug_mode = config.debug_mode;
    }
    if (config.encoding) {
        opts.encoding = config.encoding;
    }
    logger.debug("注册数据库连接池 redisName[ %s ] configParams[ %j ] redisConfiguration[ %j ]", redisName, config, opts);

    redis.createRedis(opts);
}

/**
 * 设置分数
 *
 * @param {String} key
 * @param {Number} score
 * @param {String | Number} u_id
 * @param {Function} next
 */
var setScore = function (key, score, u_id, next) {
    logger.debug('Redis Set Score key[ %s ] uid[ %s ] score[ %s ]', key, u_id, score);

    async.waterfall([
        function (cb) {
            redis.execute(redisName, function (client, release) {
                //logger.debug("get redis connection resource redisName[ %s ] client[ %s ] release[ %j ]", redisName, client, release);
                if (!client) {
                    logger.error("get redis connection resource [error] redisName[ %s ] client[ %s ] release[ %j ]", redisName, client, release);
                    cb(new Error("Get Client Error"), client, release);
                    return;
                }
                cb(null, client, release);
            });
        },
        function (client, release, cb) {
            client.zadd(key, score, u_id, function (err, data) {
                logger.debug("zadd score to overcome old value key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] data[ %j ]",
                    key, u_id, score, client, release, err, data);
                if (err) {
                    logger.error("zadd score to overcome old value [error] key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] data[ %j ]",
                        key, u_id, score, client, release, err, data);
                    cb(new Error("zadd score to db error"), client, release);
                    return;
                }
                if (!data) {
                    logger.debug("zadd score to overcome old value [error] key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] data[ %j ]",
                        key, u_id, score, client, release, err, data);
                }
                cb(null, client, release, data);
            });
        }
    ], function (err, client, release, data) {
        release();
        if (err) {
            logger.error("setScore error [ %s ]", err);
            next(err);
        } else {
            logger.debug("setScore succeed return[ %s ]", data);
            next(null, data);
        }
    });

};

/**
 * 获取排名
 *
 * @param {String} key
 * @param {String | Number} u_id
 * @param {Number} score
 * @param {Function} next
 */
var getRank = function (key, u_id, score, next) {
    logger.debug('Redis Get Rank key[ %s ] uid[ %s ] score[ %s ]', key, u_id, score);

    async.waterfall([
        // 获取数据库连接
        function (cb) {
            redis.execute(redisName, function (client, release) {
                //logger.debug("get redis connection resource redisName[ %s ] client[ %s ] release[ %j ]", redisName, client, release);
                if (!client) {
                    logger.error("get redis connection resource [error] redisName[ %s ] client[ %s ] release[ %j ]", redisName, client, release);
                    cb(new Error("Get Client Error"), client, release);
                    return;
                }
                cb(null, client, release);
            });
        },
        // 查看默认排名
        function (client, release, cb) {
            client.zrevrank(key, u_id, function (err, index) {
                logger.debug("get rank order by score from large to small key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] index[ %j ]",
                    key, u_id, score, client, release, err, index);
                if (err || typeof index != "number" || index < 0) {
                    logger.error("get rank order by score from large to small [error] key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] index[ %j ]",
                        key, u_id, score, client, release, err, index);
                    cb(new Error("get rank order by score error"), client, release);
                    return;
                }
                cb(null, client, release, index + 1);
            });
        },
        // 寻找相同分数的人
        function (client, release, index, cb) {
            client.zcount(key, score, score, function (err, count) {
                logger.debug("get same score amount key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] count[ %j ]",
                    key, u_id, score, client, release, err, count);
                if (err || !index) {
                    logger.error("get same score amount [error] key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] err[ %j ] count[ %j ]",
                        key, u_id, score, client, release, err, count);
                    cb(new Error("get same score amount error"), client, release);
                    return;
                }
                cb(null, client, release, index, count);
            });
        },
        // 将相同分数的人再次排名
        function (client, release, index, count, cb) {
            if (count > 1) {
                logger.debug("same score amount > 1 key[ %s ] uid[ %s ] score[ %s ] index[ %s ] count[ %s ] client[ %s ] release[ %j ]",
                    key, u_id, score, index, count, client, release);
                client.zrevrangebyscore(key, score, score, function (err, list) {
                    logger.debug("relist of the same score person key[ %s ] uid[ %s ] score[ %s ] index[ %s ] count[ %s ] client[ %s ] release[ %j ] err[ %j ] list[ %j ]",
                        key, u_id, score, index, count, client, release, err, list);
                    if (err) {
                        logger.error("relist of the same score person [error] key[ %s ] uid[ %s ] score[ %s ] index[ %s ] count[ %s ] client[ %s ] release[ %j ] err[ %j ] list[ %j ]",
                            key, u_id, score, index, count, client, release, err, list);
                        cb(new Error("rank same score error"), client, release);
                        return;
                    }
                    for (var i = 0; i < count; i++) {
                        if (u_id == list[i]) {
                            cb(null, client, release, index - i + 1);
                            break;
                        }
                    }
                    if (i >= count) {
                        cb(null, client, release, index);
                    }
                });
            } else {
                logger.debug("no same score key[ %s ] uid[ %s ] score[ %s ] client[ %s ] release[ %j ] count[ %j ]", key, u_id, score, client, release, count);
                cb(null, client, release, index);
            }
        }
    ], function (err, client, release, index) {
        release();
        if (err) {
            logger.error("get current rank error ", err);
            next(err);
        } else {
            logger.debug("current rank[ %s ]", index);
            next(null, index);
        }
    });
};

/**
 * 重新Hash数组组装成对象
 *
 * @param {Array} arr
 * @param {String | Number} u_id
 * @param {Number} score
 * @returns {{}}
 */
var reHashArray = function (arr, u_id, score) {
    var len = arr.length;

    var cursor = 0;
    var index = 1;

    var reHashObject = {};

    var key, value;

    var exchange = true,
        exclude = false;

    for (; cursor < len; cursor += 2) {
        key = arr[cursor];
        value = parseInt(arr[cursor + 1]);

        if (exchange && score == value) {
            reHashObject[index] = {"u_id": u_id, "score": score};
            exchange = false;
            exclude = true;
            index++;
        }

        if (exclude && u_id == key) {
            continue;
        }

        reHashObject[index] = {"u_id": key, "score": parseInt(value)};
        index++;
    }

    return reHashObject;
};

/**
 * 获取排行榜
 *
 * @param {String} key
 * @param {String | Number} u_id
 * @param {Number} score
 * @param {Number= | Function} amount
 * @param {Function | Undefined} next
 */
var getRankList = function (key, u_id, score, amount, next) {
    if (typeof amount == "function") {
        next = amount;
        amount = 30;
    }
    logger.debug("Get Rank List key[ %s ] u_id[ %s ] score[ %s ] amount[ %s ]", key, u_id, score, amount);
    async.waterfall([
        // 获取数据库连接
        function (cb) {
            redis.execute(redisName, function (client, release) {
                //logger.debug("get redis connection resource redisName[ %s ] client[ %s ] release[ %j ]", redisName, client, release);
                if (!client) {
                    logger.error("get redis connection resource [error] redisName[ %s ] client[ %s ] release[ %j ]", redisName, client, release);
                    cb(new Error("Get Client Error"), client, release);
                    return;
                }
                cb(null, client, release);
            });
        },
        // 获取排行榜
        function (client, release, cb) {
            var conditions = [key, 0, amount, 'WITHSCORES'];
            client.zrevrange(conditions, function (err, list) {
                logger.debug("get rank list redisName[ %s ] client[ %s ] release[ %j ] conditions[ %j ] err[ %j ] list[ %j ]", redisName, client, release, conditions, err, list);
                if (err) {
                    logger.error("get rank list redisName[ %s ] client[ %s ] release[ %j ] conditions[ %j ] err[ %j ] list[ %j ]", redisName, client, release, conditions, err, list);
                    cb(new Error("get rank list error"), client, release);
                    return;
                }
                var hashList = reHashArray(list, u_id, score);
                cb(null, client, release, hashList);
            });
        }
    ], function (err, client, release, ranklist) {
        release();
        if (err) {
            logger.error("get rank list error [ %s ]", err);
            next(err);
        } else {
            logger.debug("get rank list [ %j ]", ranklist);
            next(null, ranklist);
        }
    });
};

module.exports = {
    'configure': configure,
    "getRank": getRank,
    "setScore": setScore,
    "getRankList": getRankList
};
