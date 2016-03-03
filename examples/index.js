/**
 * FileName: index
 * Created by "Justin Chou <zhou78620051@126.com>".
 * On Date: 8/6/2014.
 * At Time: 10:33 AM
 */

var RankList = require("../lib/ranklist");
RankList.configure("./ranklist");
var key, u_id, score, i;

/**
 * method I: same score, first come rank higher.
 */
function SameScoreFirstComeRankHigher(){
    key = "keyI";
    u_id = "u_key143";
    score = 27;

    // Setting Score
    for (i=0;i<20;i++){
        var time = new Date().valueOf() / 10000000000000;
        RankList.setScore(key, i*2+1+time, "u_key"+(i*11), function(err, data){});
        RankList.setScore(key, i*2+1+time, "u_key"+(i*12), function(err, data){});
        RankList.setScore(key, i*2+1+time, "u_key"+(i*13), function(err, data){});
        RankList.setScore(key, i*2+1+time, "u_key"+(i*14), function(err, data){});
        RankList.setScore(key, i*2+1+time, "u_key"+(i*15), function(err, data){});
    }

    // Get The Rank
    RankList.getRank(key,u_id,score,function(err, rank){
        console.log(u_id," score is:", score, " rank is:", rank);
    });

    // Get The Rank List
    RankList.getRankList(key,u_id,score,30,function(err, list){
        console.log("rank list:\n",list);
    });
}
SameScoreFirstComeRankHigher();

/**
 * method II: the ones who got the same score, in his personal view, he ranks higher.
 */
function SameScoreIRankHigher(){
    key = "keyII";
    u_id = "u_key53";
    score = 79;

    // Setting Score
    for (i=21;i<40;i++){
        RankList.setScore(key, i*3+1, "u_key"+(i*2+1), function(err, data){});
        RankList.setScore(key, i*3+1, "u_key"+(i*2+2), function(err, data){});
        RankList.setScore(key, i*3+1, "u_key"+(i*2+3), function(err, data){});
        RankList.setScore(key, i*3+1, "u_key"+(i*2+4), function(err, data){});
        RankList.setScore(key, i*3+1, "u_key"+(i*2+5), function(err, data){});
    }

    // Get The Rank
    RankList.getRank(key,u_id,score,function(err, rank){
        console.log(u_id," score is:", score, " rank is:", rank);
    });

    // Get The Rank List
    RankList.getRankList(key,u_id,score,30,function(err, list){
        console.log("rank list:\n",list);
    });
}
SameScoreIRankHigher();
