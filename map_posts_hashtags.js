const es = require('@elastic/elasticsearch');

function getESConnection(node = "http://localhost:9200") {
  return new es.Client({
    node,
  });
}

async function getAllIndices(client) {
    const res = await client.cat.indices({
      format: "json",
    });
    return res.body?.filter((item) => item.index.startsWith("posts"));
  }

  async function getIndexMapping({ client, indexName }) {
    const res = await client.indices.getMapping({
      index: indexName,
    });
    return res.body[indexName].mappings;
  }

  async function getIndexSettings({ client, indexName }) {
    try {
      const res = await client.indices.getSettings({
        index: indexName,
      });
      const settingsData = res.body[indexName].settings?.index;
      console.log({ settingsData });
      const preparedSettings = {};
      //   if (settingsData?.number_of_shards) {
      //     preparedSettings["number_of_shards"] = settingsData?.number_of_shards;
      //   }
      //   if (settingsData?.number_of_replicas) {
      //     preparedSettings["number_of_replicas"] = settingsData?.number_of_replicas;
      //   }
      if (settingsData?.analysis) {
        preparedSettings["analysis"] = settingsData?.analysis;
      }
      if (!Object.keys(preparedSettings).length) return false;
      return { index: preparedSettings };
    } catch (error) {
      console.log(error);
      return false;
    }
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

  async function setIndexSettings(client, settings, indexName) {
    try {
      return await client.indices.putSettings({
        index: indexName,
        body: settings,
      });
    } catch (error) {
      console.log(error);
      return false;
    }
  }

  async function setIndexMappings(client, mapping, indexName, type = "_doc") {
    try {
      return await client.indices.putMapping({
        index: indexName,
        body: mapping,
        type: type,
        includeTypeName: true,
      });
    } catch (error) {
      console.log(
        `!!!!!!-------- Unable to put mapping in index :: ${indexName} --------!!!!!!`
      );
      console.log(error);
      return false;
    }
  }

  async function createIndex( node, mapping, settings, indexName ) {
    try {
     const client = new es.Client({
        node: "https://vpc-production-elastic-cluster-zsvnynzypinvuqs4igbeqpnskm.ap-south-1.es.amazonaws.com"
     })
     //console.log(`-----INDEX CREATED ON--------`, client);
      return await client.indices.create({
        index: indexName,
      });
    } catch (error) {
      console.log(
        `!!!!!!-------- Unable to create index ::: ${indexName} --------!!!!!!`
      );
      console.log(error);
      // return true;
    }
    await closeIndex(client, indexName);
    if (settings) {
      await setIndexSettings(client, settings, indexName);
    }
    await setIndexMappings(client, mapping, indexName);
    await openIndex(client, indexName);
    return true;
}

async function saveDocInIndex({client, index, doc}){
    try {
        return await client.index({
            index: index,
            type: "_doc",
            body: doc,
        })
    } catch (error) {
        console.log(error);
        return;
    }
}

async function startCloning({
    SOURCE_NODE = 'http://localhost:9200/',
    TARGET_NODE = "http://localhost:9200/"
}) {
    if (!SOURCE_NODE && !TARGET_NODE) {
        throw new Error("Target Node and Source Node are required!");
      }
    try {
        const sourceClient = getESConnection(SOURCE_NODE);
    console.log(`-------- ES Connected :: SOURCE [${SOURCE_NODE}]--------`);
    const targetClient = getESConnection(TARGET_NODE);
    console.log(`-------- ES Connected :: TARGET [${TARGET_NODE}] --------`);

    const sourceIndexesList = await getAllIndices(sourceClient);
    console.log(sourceIndexesList);

    for(let i = 0; i<sourceIndexesList.length; i++) {
        const sourceIndexName = sourceIndexesList[i]?.index;
        if(
            !sourceIndexName.includes("posts_") || sourceIndexName.includes('posts_latest')
        ){
            continue;
        }
        else {
            const sourceIndexDocLength = parseInt(
                sourceIndexesList[i]?.["docs.count"] || 0
              );
              console.log(
                `>>>>>>>>> Processing index :: ${sourceIndexName} <<<<<<<<<<`
              );
              const sourceIndexMapping = await getIndexMapping({
                client: sourceClient,
                indexName: sourceIndexName,
              });
              console.log(
                `-------- index mapping received :: ${sourceIndexName} --------`
              );
              const indexSettings = await getIndexSettings({
                client: sourceClient,
                indexName: sourceIndexName,
              });
              console.log(
                `-------- index settings received :: ${sourceIndexName} --------`
              );
              const index_data = await sourceClient.search({
                size: 30000,
                index: sourceIndexName,
              })
              const source_index_data = index_data.body?.hits?.hits;
              console.log(`------Source index data----`, source_index_data);
              for(let i = 0; i < source_index_data.length; i++){
                // get doc of posts which contains hashtag 
                if(source_index_data[i]._source.hashtags.length > 0) {
                    const doc = source_index_data[i]._source;
                    console.log(`----doc to be saved in index------`, doc);
                    const hashtags_in_posts = source_index_data[i]._source.hashtags;
                    console.log(hashtags_in_posts);
                    const created_at = source_index_data[i].created_at;
                    //loop through the hashtag array and create and save the doc on the particular index according to hashtag first alphabet
                    for(let i = 0; i < hashtags_in_posts.length; i++){
                        const indexName = `${hashtags_in_posts[i].charAt(1).toLowerCase() != hashtags_in_posts[i].charAt(1).toUpperCase() ? hashtags_in_posts[i].charAt(1) : "other"}_posts_22`.toLowerCase();
                        const does_index_exists = await targetClient.indices.exists({index: indexName});
                        //console.log(`-----Does index exists------`, does_index_exists);
                        if(!does_index_exists.body){
                        await createIndex(
                            TARGET_NODE,
                            sourceIndexMapping,
                            indexSettings,
                            indexName,
                        )
                    }
                    console.log(`---Index Name------`, indexName);
                    await saveDocInIndex({
                        client: targetClient,
                        index: indexName,
                        doc: doc
                    });
                    }
                }
            }
        }
    }



    } catch (error) {
    console.log(error);
    }
}



startCloning({
    SOURCE_NODE: "",
    TARGET_NODE: ""
})