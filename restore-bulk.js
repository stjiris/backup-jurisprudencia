const {Client} = require("@elastic/elasticsearch")
const client = new Client({node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS}})
const {createReadStream} = require("fs");
const zlib = require("node:zlib");
const tar = require("tar-stream");

let ignore = ["provided_name","creation_date","uuid","version"]

async function *extractDocuments(extract){
    for await (const entry of extract) {
        let name = entry.header.name;
        let [index, id] = name.replace(".json","").split("/");
        yield {index, id, ...JSON.parse(await streamToString(entry))}
    }
}

module.exports = async function restore(filetoTar){
    const start = new Date();
    const extract = tar.extract();

    createReadStream(filetoTar).pipe(zlib.createGunzip()).pipe(extract)

    let indice = null;
    let iter = await extract[Symbol.asyncIterator]();
    let firstEntry = (await iter.next()).value;
    let name = firstEntry.header.name;
    if( name == "indices.json" ){
        let indiceInfo = JSON.parse(await streamToString(firstEntry), (key, value) => ignore.includes(key) ? undefined : value);
        for( let indice of Object.keys(indiceInfo) ){
            let exists = await client.indices.exists({index:indice});
            if( !exists ){
                await client.indices.create({index:indice, ...indiceInfo[indice]}).then(r => console.log(`Creating ${r.index}. result: ${r.acknowledged}`))
            }
            await client.indices.putSettings({index: indice, settings: {refresh_interval: -1}})
        }
    }
    
    await client.helpers.bulk({
        datasource: extractDocuments(iter),
        onDocument(doc){
            let id = doc.id;
            let index = doc.index;
            delete doc.id;
            return { index: { _id: id, _index: index }}
        }
    })
    await client.indices.putSettings({index: indice, settings: {refresh_interval: null}})
    console.log("Ended after", new Date() - start, "ms")
}

function streamToString(stream){
    return new Promise((resolve, reject) => {
        let chunks = [];
        stream.on("data", (chunk) => {
            chunks.push(chunk.toString());
        })
        stream.on("end", () => {
            resolve(chunks.join(""));
        })
        stream.on("error", (err) => {
            reject(err);
        })
    })
}