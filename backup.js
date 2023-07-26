const {Client} = require("@elastic/elasticsearch")
const client = new Client({node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS}})
const fs = require("fs/promises")
const path = require("path");
const start = new Date();
const datetime = start.toISOString();

let index = process.env.ES_INDEX || "jurisprudencia.9.4";

let folder = path.join(index, datetime);
let valuesFolder = path.join(folder, "values")

fs.mkdir(folder, {recursive: true}).then( async () => {
    console.log("Starting backup", folder)
    await client.indices.get({index: index}).then(r => fs.writeFile(path.join(folder,"indice.json"), JSON.stringify(r)))
    console.log("Indice information writen")
    await fs.mkdir(valuesFolder, {recursive: true});
    let r = await client.search({index, size: 50, scroll: '1m', track_total_hits: true});
    let c = 0;
    while( r.hits.hits.length > 0 ){
        let promises = []
        for( let hit of r.hits.hits ){
            promises.push(fs.writeFile(path.join(valuesFolder, `${hit._id}.json`), JSON.stringify(hit._source)))
            c++;
        }

        r = await Promise.all(promises).then(_ => client.scroll({scroll: '1m',scroll_id: r._scroll_id}));
        console.log(`Backup`, c, " / ", r.hits.total.value)
    }
    console.log("Ended after", new Date() - start, "ms")
})


