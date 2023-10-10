const {Client} = require("@elastic/elasticsearch")
const client = new Client({node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS}})
const {createWriteStream} = require("fs")
const path = require("path");
const zlib = require("node:zlib");
const tar = require("tar-stream");
const { log } = require("console");

module.exports = async function backup(index){
    const start = new Date();
    const datetime = `${start.getFullYear()}-${start.getMonth()+1}-${start.getDate()}-${start.getHours()}-${start.getMinutes()}-${start.getSeconds()}`
    let r = await client.indices.get({index: index});
    console.log("Starting backup", index);
    
    let file = `${index}-${datetime}.tar.gz`;

    let tarStream = tar.pack();
    let gzStream = zlib.createGzip();
    let writeStream = createWriteStream(file);

    tarStream.pipe(gzStream).pipe(writeStream);

    tarStream.entry({name: "indice.json"}, JSON.stringify(r));
    
    let c = 0;

    r = await client.search({index, size: 50, scroll: '1m', track_total_hits: true});
    while( r.hits.hits.length > 0 ){
        for( let hit of r.hits.hits ){
            tarStream.entry({name: path.join("values", hit._id+".json")}, JSON.stringify(hit._source));
        }
        c+=r.hits.hits.length;
        r = await client.scroll({scroll: '1m',scroll_id: r._scroll_id});
        console.log(`Backup`, c, " / ", r.hits.total.value)
    }
    tarStream.finalize();
    console.log("Ended after", new Date() - start, "ms")
}

