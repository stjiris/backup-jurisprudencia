const { Client } = require("@elastic/elasticsearch")
const client = new Client({ node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS } })
const { createReadStream } = require("fs");
const zlib = require("node:zlib");
const tar = require("tar-stream");

let ignore = ["provided_name", "creation_date", "uuid", "version"]

module.exports = async function restore(filetoTar) {
    const start = new Date();
    const extract = tar.extract();

    createReadStream(filetoTar).pipe(zlib.createGunzip()).pipe(extract)
    let indices = null;
    let c = 0;
    for await (const entry of extract) {
        let name = entry.header.name;
        if (name == "indices.json") {
            let indiceInfo = JSON.parse(await streamToString(entry), (key, value) => ignore.includes(key) ? undefined : value);
            indices = Object.keys(indiceInfo);
            for (let indice of indices) {
                let exists = await client.indices.exists({ index: indice });
                if (!exists) {
                    await client.indices.create({ index: indice, ...indiceInfo[indice] }).then(r => console.log(`Creating ${header.name}. result: ${r.acknowledged}`))
                }
                await client.indices.putSettings({ index: indice, settings: { refresh_interval: -1 } })
            }
        }
        else {
            let [index, id] = name.replace(".json", "").split("/");
            let obj = JSON.parse(await streamToString(entry));
            await client.index({
                index: index,
                id: id,
                document: obj
            })
            console.log("Indexing", c++)
        }
    }
    for (let indice of indices) {
        await client.indices.putSettings({ index: indice, settings: { refresh_interval: null } })
    }
    console.log("Ended after", new Date() - start, "ms")
}

function streamToString(stream) {
    return new Promise((resolve, reject) => {
        let chunks = [];
        stream.on("data", (chunk) => {
            chunks.push(Buffer.from(chunk))
        })
        stream.on("end", () => {
            resolve(Buffer.concat(chunks).toString())
        })
        stream.on("error", (err) => {
            reject(err)
        })
    })
}