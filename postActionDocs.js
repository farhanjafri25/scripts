const es = require("@elastic/elasticsearch");
const mysql = require("mysql2/promise");
var _ = require("lodash");
const redisGrp = require("redis");
const masterNodesGrp = [
  {
    url: "redis://redis-clstr.fheoyy.clustercfg.aps1.cache.amazonaws.com:6379",
  },
];
const elasticsearchSearch = getESConnection(
  "https://vpc-es-post-actions-io6qy67quep6caduzcqopkbxc4.ap-south-1.es.amazonaws.com"
);
const clientGrp = redisGrp.createCluster({
  rootNodes: masterNodesGrp,
});
clientGrp
  .connect()
  .then(() => {
    clientGrp.set("connect", "true");
    console.log("<----------- redis connected --------------->");
    // getActionUpdate(clientGrp, "ec4bb66b-df0a-45a-bf45-f776408ee963");
    saveToDB();
  })
  .catch((err) => {
    console.log("error", err);
  });

async function connectDb() {
  const connection = await mysql.createConnection({
    host: "hood-production-rds.cluster-c7w6xkfdjdto.ap-south-1.rds.amazonaws.com",
    port: "3306",
    user: "admin",
    password: "Jo1n#ZorroPrdUser!#admin",
  });
  return connection;
}
async function getPostIds(skip, limit, connection) {
  console.log("getPostIds", skip, limit);
  const q = `Select post_id from action_orm.posts where posts.is_deleted = 0 limit ${limit} offset ${skip}`;
  const data = await connection.execute(q);
  console.log("getPostIds", data[0].length);
  return data[0];
}

async function convertArrayToString(array) {
  return array
    .map(function (a) {
      return "'" + a.replace("'", "''") + "'";
    })
    .join();
}

async function getCommentCount(postIds, connection) {
  const commentQuery = `SELECT count(*) as count, post_id from action_orm.comment where post_id in (${postIds}) AND is_deleted=0 group by post_id`;
  const commentData = await connection.execute(commentQuery);
  return commentData[0];
}
async function getPostMetrics(postIds, connection) {
  const query = `SELECT 
    p.post_id,
    SUM(p.activity_type = 'LIKED') AS likeCount,
	SUM(p.activity_type = 'DISLIKED') AS dislikeCount,
    SUM(p.activity_type = 'SCREENSHOT') AS screenshotCount

FROM
    action_orm.user_activity p
        INNER JOIN
    users.users u ON u.id = p.user_id
WHERE
         post_id in (${postIds})
        AND u.country_code != '00'
GROUP BY p.post_id
ORDER BY p.post_id`;
  const data = await connection.execute(query);
  return data[0];
}

function getESConnection(node = "http://localhost:9200") {
  return new es.Client({
    node,
  });
}

async function setPostDocs(data) {
  const docs = [];
  const nonExistingPostIds = [];
  const chunkSize = 1000;
  for (let i = 0; i < data.length; i += chunkSize) {
    const postId = data[i].post_id;
    docs.push({ _index: "post_action_count", _id: postId });
    const dataArray = await Promise.allSettled([
      elasticsearchSearch.exists({
        index: "post_action_count",
        id: postId,
      }),
    ]);
    if (result.body === false) {
      nonExistingPostIds.push(postId);
    }
  }
  return nonExistingPostIds;
}
async function saveToDB() {
  const nonExistingPostIds = [];
  let page = 0;
  let pageSize = 20000;
  const connection = await connectDb();
  while (true) {
    console.log("Page : ", page);
    const data = await getPostIds(page * pageSize, pageSize, connection);
    console.log("GetPostIds", data.length);
    if (data.length <= 0) break;
    const postIdsArray = await setPostDocs(data);
    nonExistingPostIds.push(...postIdsArray);
    console.log("Arrray Returned:", "Page : ", page, nonExistingPostIds.length);
    page = page + 1;
  }
  const finalArr = await convertArrayToString(nonExistingPostIds);
  const postActionMetricsArray = await getPostMetrics(finalArr, connection);
  const commentCountArray = await getCommentCount(finalArr, connection);
  const payload = [];
  for (let i = 0; i < nonExistingPostIds.length; i++) {
    const updateActionObject = {
      postId: nonExistingPostIds[i],
      likeCount: 0,
      dislikeCount: 0,
      screenshotCount: 0,
      viewCount: 0,
      commentCount: 0,
      dummylikeCount: 0,
      dummydislikeCount: 0,
      dummyscreenshotCount: 0,
      dummycommentCount: 0,
      dummyviewCount: 0,
    };
    const isCommentCountExist = _.find(commentCountArray, {
      post_id: nonExistingPostIds[i],
    });
    const isActionCountExist = _.find(postActionMetricsArray, {
      post_id: nonExistingPostIds[i],
    });
    if (isCommentCountExist)
      updateActionObject["commentCount"] = +isCommentCountExist.count;
    if (isActionCountExist) {
      updateActionObject["likeCount"] = +isActionCountExist.likeCount;
      updateActionObject["dislikeCount"] = +isActionCountExist.dislikeCount;
      updateActionObject["screenshotCount"] =
        +isActionCountExist.screenshotCount;
    }
    updateActionObject["viewCount"] =
      +updateActionObject.likeCount +
      +updateActionObject.dislikeCount +
      +updateActionObject.screenshotCount +
      +updateActionObject.commentCount;
    const key = `{Post_Action_Count_Set}:${nonExistingPostIds[i]}`;
    const redisObject = await clientGrp.hGetAll(key);
    if (redisObject.postId) {
      console.log("redis updateActionObject", redisObject);
      const actionFields = [
        "likeCount",
        updateActionObject.likeCount ?? 0,
        "dislikeCount",
        updateActionObject.dislikeCount ?? 0,
        "commentCount",
        updateActionObject.commentCount ?? 0,
        "screenshotCount",
        updateActionObject.screenshotCount ?? 0,
        "viewCount",
        updateActionObject.viewCount ?? 0,
      ];
      console.log(`redis Action Fields --->`, actionFields);
      updateActionObject["dummylikeCount"] = Number(redisObject.dummylikeCount);
      updateActionObject["dummydislikeCount"] = Number(
        redisObject.dummydislikeCount
      );
      updateActionObject["dummyscreenshotCount"] = Number(
        redisObject.dummyscreenshotCount
      );
      updateActionObject["dummycommentCount"] = Number(
        redisObject.dummycommentCount
      );
      updateActionObject["dummyviewCount"] = Number(redisObject.dummyviewCount);
      try {
        const data = await clientGrp.HSET(key, actionFields);
        console.log("updated in redis", data);
        await clientGrp.expire(key, 24 * 60 * 60);
      } catch (error) {
        console.log(error);
      }
    }
    console.log(updateActionObject);
    const body = {
      postId: `${nonExistingPostIds[i]}`,
      ...updateActionObject,
      isDeleted: false,
    };
    payload.push({
      index: {
        _index: "post_action_count",
        _type: "_doc",
        _id: nonExistingPostIds[i],
      },
    });
    payload.push(body);
  }
  const data = await elasticsearchSearch.bulk({
    index: `post_action_count`,
    body: payload,
    refresh: "wait_for",
  });
  console.log(`inseted Dataa`, JSON.stringify(data));
  connection.end();
}
