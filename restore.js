const {Client} = require("@elastic/elasticsearch")
const client = new Client({node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS}})
const fs = require("fs/promises")
const path = require("path");

let index = process.env.ES_INDEX || "jurisprudencia.9.4";

let ignore = ["provided_name","creation_date","uuid","version"]

console.log(process.env.INITIAL_OFFSET)
const INITIAL_OFFSET = parseInt(process.env.INITIAL_OFFSET) || 0

fs.readdir(index).then( async backups => {
    let last = backups.sort((a,b) => new Date(b) - new Date(a))[0]
    let indiceInfo = path.join(index, last, "indice.json");
    let indiceInfoObj = JSON.parse((await fs.readFile(indiceInfo)).toString(), (key, value) => ignore.includes(key) ? undefined : value )
    let exists = await client.indices.exists({index: index});
    if( !exists ){
        await client.indices.create({index, ...indiceInfoObj[index]}).then(r => console.log(`Creating ${index}. result: ${r.acknowledged}`))
    }
    let folder = path.join(index, last, "values");
    let files = await fs.readdir(folder);
    let i = INITIAL_OFFSET;
    for( let filename of files.slice(INITIAL_OFFSET)){
        i++;
        let id = filename.replace(".json","");
        let file = path.join(folder, filename);
        if( exists ){
            if( await client.exists({index: index, id: id}) ){
                continue;
            }
        }
        let obj = JSON.parse(await fs.readFile(file));
        await client.index({
            index: index,
            id: id,
            document: obj
        })
        console.log(`Index ${i} / ${files.length}`)
    }
    
})