const es = require("@elastic/elasticsearch");

async function setWindowSettingsOnIndex({
  client,
  indexName,
  windowSize = 10000,
}) {
  return await client.indices.putSettings({
    index: indexName,
    body: {
      max_result_window: windowSize,
    },
  });
}

async function getIndexLength(client, index) {
  return await client.count({
    index: index,
  });
}

async function checkIndexExists(client, index) {
  const response = await client.indices.exists({
    index: index,
  });
  return response?.body;
}

async function createIndex(client, index) {
  return await client.indices.create({
    index: index,
  });
}

const closeIndex = async (client, indexName) => {
  return await client.indices.close({
    index: indexName,
  });
};

const openIndex = async (client, indexName) => {
  return await client.indices.open({
    index: indexName,
  });
};

async function saveBulk({ client, indexName, docsList = [] }) {
  try {
    if (!docsList || !docsList?.length) return false;
    const payload = [];
    for (const item of docsList) {
      //   console.log(
      //     { _index: indexName, _type: item._type, _id: item._id },
      //     item?._source
      //   );
      payload.push({
        index: { _index: indexName, _type: item._type, _id: item._id },
      });
      payload.push(item?._source);
    }
    // console.log(JSON.stringify(payload));
    return await client.bulk({
      index: indexName,
      body: payload,
      refresh: "wait_for",
    });
  } catch (error) {
    return false;
  }
}

async function createIndexWithPostMapping(client, indexName) {
  const creationResponse = await createIndex(client, indexName);
  await closeIndex(client, indexName);
  await putIndexSettings(client, indexName);
  await putMappingInIndex(client, indexName, "_doc");
  await openIndex(client, indexName);
  return creationResponse;
}

async function putIndexSettings(client, index) {
  const settings = {
    settings: {
      index: {
        // number_of_shards: 5,
        // number_of_replicas: 1,
        analysis: {
          analyzer: {
            englishAnalyzer: {
              type: "standard",
              stopwords: "_english_",
            },
          },
        },
      },
    },
  };
  return await client.indices.putSettings({
    index: index,
    body: settings,
  });
}

async function getPostsFromSource(client, indexName, startTime, endTime) {
  try {
    const query = {
      sort: [
        {
          createdAt: {
            order: "asc",
          },
        },
      ],
      query: {
        bool: {
          must: [
            {
              range: {
                createdAt: {
                  gte: `${startTime}`,
                  lte: `${endTime}`,
                },
              },
            },
          ],
        },
      },
    };
    const response = await client.search({
      index: indexName,
      body: query,
      from: 0,
      size: 50000,
    });
    console.log({ response });
    if (!response?.body?.hits?.hits?.length) {
      return [];
    }
    return response?.body?.hits?.hits;
  } catch (error) {
    console.log(error);
  }
}

async function startMigration({
  SOURCE_NODE = "http://localhost:9200",
  TARGET_NODE = "http://localhost:9200",
}) {
  const sourceIndexName = "posts";
  try {
    const sourceClient = new es.Client({
      node: SOURCE_NODE,
    });
    console.log(`-------- ES Connected :: SOURCE [${SOURCE_NODE}]--------`);
    const targetClient = new es.Client({
      node: TARGET_NODE,
    });
    console.log(`-------- ES Connected :: TARGET [${TARGET_NODE}] --------`);

    const indexLengthResponse = await getIndexLength(
      sourceClient,
      sourceIndexName
    );
    console.log(
      `-------- Total length of source Index :: ${
        indexLengthResponse?.body?.count || indexLengthResponse
      } --------`
    );

    for (let i = 1; i <= 12; i = i + 3) {
      // 3 months range createdAt
      const startMonthNum = i;
      const endMonthNum = i + 3;
      if (startMonthNum >= 10) {
        return;
      }
      const startMonthNumStr = `0${startMonthNum}`.slice(-2);
      const endMonthNumStr = `0${endMonthNum}`.slice(-2);
      const startTimeISO = `2022-${startMonthNumStr}-01T00:00:00.000Z`;
      const endTimeISO = `2022-${endMonthNumStr}-01T00:00:00.000Z`;

      const endMonthNumStr2 = `0${i + 2}`.slice(-2);

      const startDateObj = new Date(startTimeISO);
      const endDateObj = new Date(`2022-${endMonthNumStr2}-01T00:00:00.000Z`);

      const startMonthName = startDateObj.toLocaleString("en-IN", {
        month: "short",
      });
      const endMonthName = endDateObj.toLocaleString("en-IN", {
        month: "short",
      });
      const startDateYear = startDateObj.toLocaleString("en-IN", {
        year: "2-digit",
      });
      const endDateYear = endDateObj.toLocaleString("en-IN", {
        year: "2-digit",
      });
      const newIndexName =
        `posts_${startMonthName}${startDateYear}_${endMonthName}${endDateYear}`.toLowerCase();
      console.log(
        `-------------- New Index Name :: ${newIndexName} --------------`
      );
      await setWindowSettingsOnIndex({
        client: sourceClient,
        indexName: sourceIndexName,
        windowSize: 50000,
      });

      const sourcePostsResponse = await getPostsFromSource(
        sourceClient,
        sourceIndexName,
        startTimeISO,
        endTimeISO
      );
      console.log(
        `-------- Source posts list :: ${sourcePostsResponse} --------`
      );
      const indexExists = await checkIndexExists(targetClient, newIndexName);
      console.log(`-------- Does Index Exist :: ${indexExists} --------`);

      // if !exists
      if (!indexExists) {
        // then create index
        const createIndexResponse = await createIndexWithPostMapping(
          targetClient,
          newIndexName
        );
        console.log(
          `-------- Create index response :: ${createIndexResponse} --------`
        );
      }

      if (!sourcePostsResponse.length) {
        console.log(`-------No Posts Found in the range-------`);
        continue;
      }

      // prepare bulk object
      // then start bulk save
      const saveResponse = await saveBulk({
        client: targetClient,
        indexName: newIndexName,
        docsList: sourcePostsResponse,
      });
      console.log(`------- Bulk Save Response ---------`);
      console.log({ saveResponse: saveResponse?.body });

      await setWindowSettingsOnIndex({
        client: sourceClient,
        indexName: sourceIndexName,
        windowSize: 9999,
      });
    }
    console.log("------- ✅ DONE -------");
  } catch (error) {
    console.log("Error: ");
    console.log(error);
  }
}

async function putMappingInIndex(client, index, type = "_doc") {
  const mapping = {
    properties: {
      category: {
        type: "keyword",
      },
      createdAt: {
        type: "date",
      },
      cursor: {
        type: "keyword",
      },
      hashtags: {
        type: "keyword",
      },
      isAbusive: {
        type: "boolean",
      },
      isDeleted: {
        type: "boolean",
      },
      isPostActive: {
        type: "boolean",
      },
      isTagsApproved: {
        type: "boolean",
        null_value: false,
      },
      isTagsSubmitted: {
        type: "boolean",
        null_value: false,
      },
      isUserActive: {
        type: "boolean",
      },
      isUserVerified: {
        type: "boolean",
      },
      mention: {
        properties: {
          userId: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          username: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
        },
      },
      mentions: {
        type: "keyword",
      },
      post: {
        type: "text",
        analyzer: "englishAnalyzer",
      },
      postId: {
        type: "keyword",
      },
      post_keyword: {
        type: "keyword",
      },
      tags: {
        type: "nested",
      },
      tagsUpdatedAt: {
        type: "date",
      },
      topics: {
        type: "keyword",
      },
      updatedAt: {
        type: "date",
      },
      userId: {
        type: "keyword",
      },
      userIndustry: {
        type: "keyword",
      },
      userLocation: {
        type: "geo_point",
      },
      userProfilePic: {
        type: "text",
      },
      username: {
        type: "text",
      },
      username_keyword: {
        type: "keyword",
      },
    },
  };
  return await client.indices.putMapping({
    index: index,
    type: type,
    body: mapping,
    includeTypeName: true,
  });
}


startMigration({
  SOURCE_NODE:
    "",
  TARGET_NODE:
	""
});
