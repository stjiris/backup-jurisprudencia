const {Client} = require("@elastic/elasticsearch")
const client = new Client({node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS}})
const {createWriteStream} = require("fs")
const path = require("path");
const zlib = require("node:zlib");
const tar = require("tar-stream");
const { log } = require("console");

module.exports = async function backup(indexPattern){
    const start = new Date();
    const datetime = `${start.getFullYear()}-${start.getMonth()+1}-${start.getDate()}-${start.getHours()}-${start.getMinutes()}-${start.getSeconds()}`
    let r = await client.indices.get({index: indexPattern});
    let index = Object.keys(r);
    console.log("Starting backup", index.join(", "));
    
    let file = `${datetime}.tar.gz`;

    let tarStream = tar.pack();
    let gzStream = zlib.createGzip();
    let writeStream = createWriteStream(file);

    tarStream.pipe(gzStream).pipe(writeStream);

    tarStream.entry({name: "indices.json"}, JSON.stringify(r));
    
    let c = 0;

    r = await client.search({index, size: 50, scroll: '1m'});
    while( r.hits.hits.length > 0 ){
        for( let hit of r.hits.hits ){
            tarStream.entry({name: path.join(hit._index, hit._id+".json")}, JSON.stringify(hit._source));
        }
        c+=r.hits.hits.length;
        r = await client.scroll({scroll: '1m',scroll_id: r._scroll_id});
        console.log(`Backup`, c)
    }
    await client.clearScroll({scroll_id: r._scroll_id});
    tarStream.finalize();
    console.log("Ended after", new Date() - start, "ms")
}

