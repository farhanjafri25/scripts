const mysql = require("mysql2/promise");
const redis = require('redis');

const masterNodes = [
  {
    url: "",
  },
];
const client = redis.createCluster({
  rootNodes: masterNodes,
});

client
    .connect()
    .then(() => {
        client.set("connect", "true");
        console.log("<----------- redis connected --------------->");
        saveToDB(0, 1000)
    })
    .catch((err) => {
        console.log("error", err);
    });


    async function getDocsCount(skip, limit) {
        const connection = await connectDb();
        const q = `SELECT count(id) as count FROM users.users where is_deleted = 0`;
        const data = await connection.execute(q);
        connection.end();
        return data[0][0];
      }

    async function connectDb() {
        const connection = await mysql.createConnection({
          host: "",
          port: "",
          user: "",
          password: "",
        });
        return connection;
    }

async function getUsers(skip, limit) {
    const connection = await connectDb();
    const query = `select id from users.users order by created_at desc limit ${limit} offset ${skip}`;
    const data = await connection.execute(query);
    connection.end();
    return data[0];
}

async function getUserGroups(user_id) {
    const conection = await connectDb();
    const query = `SELECT * FROM users.groups_members left join users.groups on groups_members.group_id = users.groups.group_id where member_id = "${user_id}" and groups.is_deleted = 0`;
    const data = await conection.execute(query);
    conection.end();
    return data[0];
}


async function saveGroupObjToRedis(skip,limit) {
    const userIds = await getUsers(skip, limit);
    for(let i = 0; i<userIds.length; i++) {
        console.log(`UserId --->`, userIds[i]);
        const userGroups = await getUserGroups(userIds[i].id);
        console.log(`UserGroups --->`, userGroups);
        if(userGroups.length > 0) {
            const userGroup = [];
            for(let i = 0; i<userGroups.length; i++) {
                const payload = {
                    groupId : userGroups[i].group_id,
                    groupImage: userGroups[i].group_image,
                    description: userGroups[i].description,
                    memberCount: userGroups[i].member_count
                }
                userGroup.push(payload);
            }
            const getUser = await client.get(`{user}:${userIds[i].id}`);
                const parsedData = JSON.parse(getUser);
                const payload = {
                    ...parsedData,
                    userGroups: userGroup
                }
            console.log(`UserGroups payload--->`, payload);
            const stringifiedData = JSON.stringify(payload);
            // const res = await client.set(`{user}:${userIds[i].id}`, stringifiedData);
            // console.log(res);
        }
    }
} 

async function saveToDB(skip, limit) {
    const getAllRecordCount = await getDocsCount();
    console.log(`getAllRecordCount`, getAllRecordCount.count);
    while (skip < getAllRecordCount.count) {
    await saveGroupObjToRedis(skip, limit);
    skip = skip + limit;
    limit = 1000;
    }
  }