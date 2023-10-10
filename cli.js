const { Client } = require("@elastic/elasticsearch")
const { stat } = require("fs/promises")
const backup = require("./backup")
const restore = require("./restore-bulk")

const client = new Client({node: process.env.ES_URL || "http://localhost:9200", auth: { username: process.env.ES_USER, password: process.env.ES_PASS}})

async function showHelp(code, error){
    if( error ){
        console.error(error)
    }

    console.log(`Usage: node cli.js <command>`)
    console.log(`Backup or restore an index from an elasticsearch instance`)
    console.log(`Use ES_URL, ES_USER and ES_PASS environment variables to setup the elasticsearch client`)
    console.log(`Available commands:`)
    console.log(`  backup  <index>\tCreate a tar file with the index information and all the documents`)
    console.log(`  restore <tar>  \tRestore an index from a tar file`)
    console.log(`Available indices to backup:`)
    await client.indices.get({index: "_all"}).then(r => console.log(Object.keys(r).join(", ")))

    process.exit(code)
}   

async function main(command, indexOrPath){
    if( !command ){
        await showHelp(0);
    }
    if( !indexOrPath ){
        if( command == "backup" ){
            await showHelp(1, "Please provide an index to backup")
        }
        else if( command == "restore" ){
            await showHelp(1, "Please provide a tar file to restore")
        }
        else {
            await showHelp(1, `Unknown command "${command}"`)
        }   
    }

    if( command == "backup" ){
        let exists = await client.indices.exists({index: indexOrPath});
        if( !exists ){
            await showHelp(1, `Index "${indexOrPath}" does not exists`)
        }
        await backup(indexOrPath)
    }
    else if( command == "restore" ){
        let exists = await stat(indexOrPath);
        if( !exists ){
            await showHelp(1, `File "${indexOrPath}" does not exists`)
        }
        await restore(indexOrPath)
    }
    else {
        await showHelp(1, `Unknown command "${command}"`)
    }

}

main(process.argv[2], process.argv[3]).catch(e => showHelp(1, e))

